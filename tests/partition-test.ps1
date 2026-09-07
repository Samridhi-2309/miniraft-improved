<#
.SYNOPSIS
    Network partition (split-brain) test for the 5-node RAFT cluster.
    PowerShell-native - no jq, no Git Bash.

.DESCRIPTION
    Killing a node is not a partition. A killed node stops; a PARTITIONED leader
    keeps running and still believes it leads. That is where split-brain becomes
    possible, and it is what this test exercises.

    Isolates 2 nodes (including the current leader) from the other 3, then verifies:
      1. The majority side (3 nodes) elects a new leader
      2. The new term is strictly higher than the old one
      3. The majority side can still commit
      4. After healing, exactly ONE leader remains - the old one stepped down
      5. All 5 replicas converge on an identical committed prefix
      6. The write attempted on the minority side is NOT in the committed log

    NOTE ON METHOD: once a container is disconnected from raft-network it loses
    its IP there, so its published host port stops working. That is realistic -
    it is partitioned - but it means we cannot probe the minority side over HTTP
    mid-partition. Instead we verify no split-brain commit the stronger way:
    after healing, the minority's write must be absent from the converged log.

.NOTES
    Run from the repo root:
        powershell -ExecutionPolicy Bypass -File tests\partition-test.ps1

    Run this only AFTER failure-tests.ps1 passes. It depends on elections working.
#>

$ErrorActionPreference = 'Continue'

$Compose  = @('compose', '-f', 'docker-compose.5node.yml')
$Ports    = @(4001, 4002, 4003, 4004, 4005)
$Network  = 'raft-network'
$Failures = 0

# ---------------------------------------------------------------- helpers

function Write-Header($Text) {
    Write-Host ''
    Write-Host "=== $Text ===" -ForegroundColor Cyan
}

function Check($Passed, $Message) {
    if ($Passed) { Write-Host "  PASS: $Message" -ForegroundColor Green }
    else         { Write-Host "  FAIL: $Message" -ForegroundColor Red; $script:Failures++ }
}

function Get-PortName($Port) { return "replica$($Port - 4000)" }

function Get-ReplicaState($Port) {
    try   { return Invoke-RestMethod -Uri "http://localhost:$Port/state" -TimeoutSec 3 }
    catch { return $null }
}

function Get-CommittedPrefix($Port) {
    try   { $r = Invoke-RestMethod -Uri "http://localhost:$Port/log" -TimeoutSec 3 }
    catch { return $null }

    $commitIndex = [int]$r.commitIndex
    if ($commitIndex -lt 0 -or $null -eq $r.log) { return '[]' }

    $entries = @($r.log)
    if ($entries.Count -eq 0) { return '[]' }

    $upper  = [Math]::Min($commitIndex, $entries.Count - 1)
    $prefix = @($entries[0..$upper])
    $norm   = $prefix | ForEach-Object {
        [pscustomobject]@{ index = $_.index; term = $_.term; command = $_.command }
    }
    return ($norm | ConvertTo-Json -Depth 10 -Compress)
}

function Wait-ForLeader($TimeoutSeconds = 30, $OnlyPorts = $Ports) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        foreach ($p in $OnlyPorts) {
            $s = Get-ReplicaState $p
            if ($null -ne $s -and $s.role -eq 'leader') { return $p }
        }
        Start-Sleep -Milliseconds 500
    }
    return $null
}

function Send-CommandTo($Port, $Value) {
    $body = @{ command = @{ type = 'test'; value = $Value } } | ConvertTo-Json -Depth 5
    try {
        $r = Invoke-RestMethod -Uri "http://localhost:$Port/command" -Method Post `
                               -ContentType 'application/json' -Body $body -TimeoutSec 5
        return ($r.ok -eq $true)
    } catch { return $false }
}

# ---------------------------------------------------------------- preflight

Write-Header 'Preflight'
docker version --format '{{.Server.Version}}' 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host 'Docker is not running.' -ForegroundColor Red; exit 1 }
if (-not (Test-Path 'docker-compose.5node.yml')) {
    Write-Host 'Run this from the repo root.' -ForegroundColor Red; exit 1
}
Write-Host '  OK.'

Write-Header 'Resetting cluster state'
& docker @Compose down -v 2>&1 | Out-Null      # -v also clears named volumes
& docker @Compose up -d   2>&1 | Out-Null
Write-Host '  Waiting for initial election...'

$leaderPort = Wait-ForLeader -TimeoutSeconds 40
if ($null -eq $leaderPort) {
    Write-Host '  FAIL: no leader at startup. Fix elections before running this test.' -ForegroundColor Red
    exit 1
}
$leaderName = Get-PortName $leaderPort
$termBefore = [int](Get-ReplicaState $leaderPort).currentTerm
Check $true "leader elected: $leaderName (term $termBefore)"

# ---------------------------------------------------------------- baseline

Write-Header 'Baseline: commit with all 5 connected'
$ok = Send-CommandTo $leaderPort 'partition-baseline'
Start-Sleep -Seconds 3
Check $ok 'baseline command committed'

# ---------------------------------------------------------------- partition
# Minority = leader + one follower.  Majority = the other three.

$minorityPorts = @($leaderPort, $Ports[((($leaderPort - 4001) + 1) % 5)])
$majorityPorts = $Ports | Where-Object { $minorityPorts -notcontains $_ }
$minorityNames = $minorityPorts | ForEach-Object { Get-PortName $_ }
$majorityNames = $majorityPorts | ForEach-Object { Get-PortName $_ }

Write-Header "Partitioning: isolating $($minorityNames -join ', ') from $($majorityNames -join ', ')"
Write-Host "  Minority side holds the current leader ($leaderName)."

foreach ($n in $minorityNames) {
    docker network disconnect $Network $n 2>&1 | Out-Null
    Write-Host "  disconnected $n"
}

Write-Host '  Waiting for the majority side to elect a new leader...'
$newLeaderPort = Wait-ForLeader -TimeoutSeconds 40 -OnlyPorts $majorityPorts

Check ($null -ne $newLeaderPort) `
      "majority side (3/5) elected a new leader$(if ($newLeaderPort) { ": $(Get-PortName $newLeaderPort)" })"

if ($null -ne $newLeaderPort) {
    $termAfter = [int](Get-ReplicaState $newLeaderPort).currentTerm
    Check ($termAfter -gt $termBefore) "term advanced ($termBefore -> $termAfter)"
}

Write-Header 'Minority side must be unreachable from the host'
$minorityReachable = $false
foreach ($p in $minorityPorts) {
    if ($null -ne (Get-ReplicaState $p)) {
        Write-Host "  $(Get-PortName $p): still reachable" -ForegroundColor Yellow
        $minorityReachable = $true
    } else {
        Write-Host "  $(Get-PortName $p): unreachable (expected - it is partitioned)"
    }
}
if ($minorityReachable) {
    Write-Host '  Note: still reachable means the partition did not fully take effect.' -ForegroundColor Yellow
}

# ---------------------------------------------------------------- writes

Write-Header 'Majority side must still commit'
if ($null -ne $newLeaderPort) {
    $before = [int](Get-ReplicaState $newLeaderPort).commitIndex
    $ok = Send-CommandTo $newLeaderPort 'majority-write'
    Start-Sleep -Seconds 3
    $after = [int](Get-ReplicaState $newLeaderPort).commitIndex
    Check $ok 'majority side accepted a write'
    Check ($after -gt $before) "majority side advanced commitIndex ($before -> $after)"
}

Write-Header 'Attempting a write on the minority side'
$minorityAccepted = $false
foreach ($p in $minorityPorts) {
    if (Send-CommandTo $p 'minority-write') { $minorityAccepted = $true }
}
if ($minorityAccepted) {
    Write-Host '  Minority leader ACCEPTED the write into its local log.'
    Write-Host '  That alone is fine - RAFT allows the append. What must never happen'
    Write-Host '  is that it COMMITS. Verified after the heal below.'
} else {
    Write-Host '  Minority side did not accept the write (unreachable or not leader).'
}

# ---------------------------------------------------------------- heal

Write-Header 'Healing the partition'
foreach ($n in $minorityNames) {
    docker network connect $Network $n 2>&1 | Out-Null
    Write-Host "  reconnected $n"
}

# docker network connect restores peer connectivity but NOT the published host
# port mapping - docker-proxy was bound to the container's old network IP.
# A restart rebuilds it. State lives in named volumes, so the log survives and
# the convergence check below is still meaningful.
Write-Host '  Restarting reconnected nodes to restore host port mapping...'
& docker @Compose restart $minorityNames[0] $minorityNames[1] 2>&1 | Out-Null
Start-Sleep -Seconds 8
Write-Host '  Waiting for the cluster to reconcile...'
Start-Sleep -Seconds 20

Write-Header 'Post-heal state'
$leaderCount = 0
foreach ($p in $Ports) {
    $s = Get-ReplicaState $p
    $role = if ($null -eq $s) { 'unreachable' } else { $s.role }
    $t    = if ($null -eq $s) { 'n/a' } else { $s.currentTerm }
    $ci   = if ($null -eq $s) { 'n/a' } else { $s.commitIndex }
    Write-Host "  $(Get-PortName $p): role=$role term=$t commitIndex=$ci"
    if ($role -eq 'leader') { $leaderCount++ }
}
Check ($leaderCount -eq 1) "exactly one leader after heal (found $leaderCount) - the old leader stepped down"

Write-Header 'All 5 replicas converge on an identical committed prefix'
$reference = Get-CommittedPrefix $Ports[0]
$allMatch  = $true
foreach ($p in $Ports) {
    $prefix = Get-CommittedPrefix $p
    if ($null -eq $prefix)        { Write-Host "  $(Get-PortName $p): unreachable" -ForegroundColor Red; $allMatch = $false; continue }
    if ($prefix -ne $reference)   { Write-Host "  $(Get-PortName $p): DIFFERS"     -ForegroundColor Red; $allMatch = $false }
    else                          { Write-Host "  $(Get-PortName $p): matches" }
}
Check $allMatch 'all 5 replicas converged'

Write-Header 'The minority write must NOT be in the committed log'
$values = @()
try {
    $final = Invoke-RestMethod -Uri "http://localhost:$($Ports[0])/log" -TimeoutSec 3
    $ci = [int]$final.commitIndex
    $entries = @($final.log)
    if ($ci -ge 0 -and $entries.Count -gt 0) {
        $upper = [Math]::Min($ci, $entries.Count - 1)
        $values = @($entries[0..$upper] | ForEach-Object { $_.command.value } | Where-Object { $_ })
    }
} catch { }

Check ($values -contains 'partition-baseline') "baseline write survived the partition"
Check ($values -contains 'majority-write')     "majority-side write is committed"
Check (-not ($values -contains 'minority-write')) "minority-side write was NOT committed (no split-brain)"

Write-Host ''
Write-Host '  Committed values:'
foreach ($v in $values) { Write-Host "    $v" }

# ---------------------------------------------------------------- summary

Write-Host ''
if ($Failures -eq 0) {
    Write-Host '==========================================' -ForegroundColor Green
    Write-Host ' PARTITION TEST PASSED'                      -ForegroundColor Green
    Write-Host ' Majority stayed available, minority could'  -ForegroundColor Green
    Write-Host ' not commit, cluster reconverged on heal.'   -ForegroundColor Green
    Write-Host '==========================================' -ForegroundColor Green
    exit 0
} else {
    Write-Host '==========================================' -ForegroundColor Red
    Write-Host " FAILED - $Failures check(s) did not pass"   -ForegroundColor Red
    Write-Host '==========================================' -ForegroundColor Red
    Write-Host ''
    Write-Host 'Ensuring no container is left disconnected:'
    foreach ($n in $minorityNames) { docker network connect $Network $n 2>&1 | Out-Null }
    & docker @Compose logs --tail=40
    exit 1
}