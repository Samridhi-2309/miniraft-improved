<#
.SYNOPSIS
    Five-node RAFT failure and recovery test. PowerShell-native, no jq required.

.DESCRIPTION
    Verifies the claim "operational with up to 2 simultaneous node failures":
      1. Cluster elects a leader and commits with all 5 nodes healthy
      2. All 5 replicas agree on the committed prefix
      3. SIGKILL the LEADER plus one follower (the hard case, not two followers)
      4. Surviving 3 elect a new leader   (quorum = 3, so this must work)
      5. Cluster still accepts and commits writes with 2 nodes down
      6. Killed nodes restart and catch up
      7. Exactly one leader afterwards
      8. All 5 committed prefixes identical, and no command was lost

.NOTES
    Run from the repo root in PowerShell:
        powershell -ExecutionPolicy Bypass -File tests\failure-tests.ps1

    Requires Docker Desktop running. Exits 0 on pass, 1 on failure.
#>

$ErrorActionPreference = 'Continue'

$Compose     = @('compose', '-f', 'docker-compose.5node.yml')
$Ports       = @(4001, 4002, 4003, 4004, 4005)
$TestPrefix  = 'failure-test'
$Failures    = 0

# ---------------------------------------------------------------- helpers

function Write-Header($Text) {
    Write-Host ''
    Write-Host "=== $Text ===" -ForegroundColor Cyan
}

function Check($Passed, $Message) {
    if ($Passed) {
        Write-Host "  PASS: $Message" -ForegroundColor Green
    } else {
        Write-Host "  FAIL: $Message" -ForegroundColor Red
        $script:Failures++
    }
}

function Get-ReplicaState($Port) {
    try   { return Invoke-RestMethod -Uri "http://localhost:$Port/state" -TimeoutSec 3 }
    catch { return $null }
}

function Get-ReplicaLog($Port) {
    try   { return Invoke-RestMethod -Uri "http://localhost:$Port/log" -TimeoutSec 3 }
    catch { return $null }
}

# RAFT guarantees replicas agree on the COMMITTED PREFIX, not on the full log.
# A follower can legitimately lag by an uncommitted entry, so comparing whole
# logs produces false failures. We derive the prefix here rather than relying
# on a 'committed' field, so this works whatever shape /log returns.
function Get-CommittedPrefix($Port) {
    $r = Get-ReplicaLog $Port
    if ($null -eq $r) { return $null }

    $commitIndex = [int]$r.commitIndex
    if ($commitIndex -lt 0 -or $null -eq $r.log) { return '[]' }

    $entries = @($r.log)
    if ($entries.Count -eq 0) { return '[]' }

    $upper = [Math]::Min($commitIndex, $entries.Count - 1)
    $prefix = @($entries[0..$upper])

    # Normalise: index, term and command are the replicated facts.
    $norm = $prefix | ForEach-Object {
        [pscustomobject]@{ index = $_.index; term = $_.term; command = $_.command }
    }
    return ($norm | ConvertTo-Json -Depth 10 -Compress)
}

function Find-LeaderPort {
    foreach ($p in $Ports) {
        $s = Get-ReplicaState $p
        if ($null -ne $s -and $s.role -eq 'leader') { return $p }
    }
    return $null
}

function Wait-ForLeader($TimeoutSeconds = 20, $ExcludePorts = @()) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        foreach ($p in $Ports) {
            if ($ExcludePorts -contains $p) { continue }
            $s = Get-ReplicaState $p
            if ($null -ne $s -and $s.role -eq 'leader') { return $p }
        }
        Start-Sleep -Milliseconds 500
    }
    return $null
}

function Send-Command($Value) {
    $port = Find-LeaderPort
    if ($null -eq $port) {
        Write-Host "    no leader available" -ForegroundColor Yellow
        return $false
    }
    $body = @{ command = @{ type = 'test'; value = $Value } } | ConvertTo-Json -Depth 5
    try {
        $r = Invoke-RestMethod -Uri "http://localhost:$port/command" -Method Post `
                               -ContentType 'application/json' -Body $body -TimeoutSec 5
        if ($r.ok -eq $true) {
            Write-Host "    '$Value' accepted by replica$($port - 4000) at index $($r.index)"
            return $true
        }
        Write-Host "    '$Value' not accepted: $($r | ConvertTo-Json -Compress)" -ForegroundColor Yellow
        return $false
    } catch {
        Write-Host "    '$Value' rejected: $($_.Exception.Message)" -ForegroundColor Yellow
        return $false
    }
}

function Get-PortName($Port) { return "replica$($Port - 4000)" }

# ---------------------------------------------------------------- preflight

Write-Header 'Preflight'

docker version --format '{{.Server.Version}}' 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host 'Docker is not running. Start Docker Desktop and retry.' -ForegroundColor Red
    exit 1
}
if (-not (Test-Path 'docker-compose.5node.yml')) {
    Write-Host 'docker-compose.5node.yml not found. Run this from the repo root.' -ForegroundColor Red
    exit 1
}
Write-Host '  Docker running, compose file found.'

# ---------------------------------------------------------------- reset
# Persistent bind mounts mean run 2 would otherwise start with run 1's log.

Write-Header 'Resetting cluster state'
& docker @Compose down 2>&1 | Out-Null
if (Test-Path './data') { Remove-Item -Recurse -Force './data' -ErrorAction SilentlyContinue }
foreach ($p in $Ports) { New-Item -ItemType Directory -Force -Path "./data/$(Get-PortName $p)" | Out-Null }
Write-Host '  Old containers removed, data directories cleared.'

# ---------------------------------------------------------------- start

Write-Header 'Starting 5-node cluster'
& docker @Compose up -d 2>&1 | Out-Null
Write-Host '  Waiting for initial election (election timeout is 1500-3000ms)...'

$leaderPort = Wait-ForLeader -TimeoutSeconds 30
if ($null -eq $leaderPort) {
    Write-Host '  FAIL: no leader elected at startup' -ForegroundColor Red
    & docker @Compose logs --tail=40
    exit 1
}
$leaderName = Get-PortName $leaderPort
Check $true "leader elected: $leaderName"

# ---------------------------------------------------------------- phase 1

Write-Header 'Phase 1: commit with all 5 nodes healthy'

$acked = 0
foreach ($i in 1..3) { if (Send-Command "$TestPrefix-before-$i") { $acked++ } }
Start-Sleep -Seconds 3

Check ($acked -eq 3) "all 3 pre-failure commands accepted (got $acked)"

Write-Header 'Phase 2: all replicas agree on the committed prefix'

$reference = Get-CommittedPrefix $Ports[0]
$allMatch  = $true
foreach ($p in $Ports) {
    $prefix = Get-CommittedPrefix $p
    if ($null -eq $prefix) { Write-Host "  $(Get-PortName $p): unreachable" -ForegroundColor Yellow; $allMatch = $false; continue }
    if ($prefix -ne $reference) { Write-Host "  $(Get-PortName $p): DIFFERS" -ForegroundColor Red; $allMatch = $false }
    else { Write-Host "  $(Get-PortName $p): matches" }
}
Check $allMatch 'all 5 replicas have identical committed prefixes'

$baseCommit = (Get-ReplicaState $leaderPort).commitIndex
Write-Host "  leader commitIndex: $baseCommit"

# ---------------------------------------------------------------- phase 3
# Kill the LEADER plus a follower. Killing two followers is the easy case and
# does not exercise re-election.

$victimA     = $leaderPort
$victimBIdx  = (($leaderPort - 4001) + 1) % 5
$victimB     = $Ports[$victimBIdx]
$victimNames = @((Get-PortName $victimA), (Get-PortName $victimB))

Write-Header "Phase 3: SIGKILL $($victimNames[0]) (the leader) and $($victimNames[1])"
Write-Host '  SIGKILL, not SIGTERM - SIGTERM allows graceful shutdown and does not test crash recovery.'

docker kill --signal=SIGKILL $victimNames[0] $victimNames[1] 2>&1 | Out-Null
Start-Sleep -Seconds 2

$survivors = $Ports | Where-Object { $_ -ne $victimA -and $_ -ne $victimB }
Write-Host "  Survivors: $(($survivors | ForEach-Object { Get-PortName $_ }) -join ', ')"

Write-Header 'Phase 4: surviving 3 must elect a new leader'
$newLeaderPort = Wait-ForLeader -TimeoutSeconds 30 -ExcludePorts @($victimA, $victimB)
Check ($null -ne $newLeaderPort) "surviving 3/5 elected a leader$(if ($newLeaderPort) { ": $(Get-PortName $newLeaderPort)" })"

if ($null -eq $newLeaderPort) {
    Write-Host '  Cannot continue without a leader.' -ForegroundColor Red
    & docker @Compose logs --tail=40
    exit 1
}

# ---------------------------------------------------------------- phase 5

Write-Header 'Phase 5: cluster must still commit with 2 nodes down'

$ackedAfter = 0
foreach ($i in 1..2) { if (Send-Command "$TestPrefix-after-$i") { $ackedAfter++ } }
Start-Sleep -Seconds 3

Check ($ackedAfter -eq 2) "cluster accepted writes with 2 simultaneous failures (got $ackedAfter/2)"

$newCommit = (Get-ReplicaState $newLeaderPort).commitIndex
Check ($newCommit -gt $baseCommit) "commitIndex advanced past the failure point ($baseCommit -> $newCommit)"

# ---------------------------------------------------------------- phase 6

Write-Header 'Phase 6: restart the killed nodes'
& docker @Compose up -d $victimNames[0] $victimNames[1] 2>&1 | Out-Null
Write-Host '  Waiting for recovered nodes to catch up...'
Start-Sleep -Seconds 15

# ---------------------------------------------------------------- phase 7

Write-Header 'Phase 7: exactly one leader'

$leaderCount = 0
foreach ($p in $Ports) {
    $s = Get-ReplicaState $p
    $role = if ($null -eq $s) { 'unreachable' } else { $s.role }
    $ci   = if ($null -eq $s) { 'n/a' } else { $s.commitIndex }
    $t    = if ($null -eq $s) { 'n/a' } else { $s.currentTerm }
    Write-Host "  $(Get-PortName $p): role=$role term=$t commitIndex=$ci"
    if ($role -eq 'leader') { $leaderCount++ }
}
Check ($leaderCount -eq 1) "exactly one leader (found $leaderCount)"

# ---------------------------------------------------------------- phase 8

Write-Header 'Phase 8: all 5 committed prefixes identical after recovery'

$reference = $null
foreach ($p in $Ports) { if ($null -eq $reference) { $reference = Get-CommittedPrefix $p } }

$allMatch = $true
foreach ($p in $Ports) {
    $prefix = Get-CommittedPrefix $p
    if ($null -eq $prefix) { Write-Host "  $(Get-PortName $p): unreachable" -ForegroundColor Red; $allMatch = $false; continue }
    if ($prefix -ne $reference) {
        Write-Host "  $(Get-PortName $p): DIFFERS" -ForegroundColor Red
        $allMatch = $false
    } else {
        Write-Host "  $(Get-PortName $p): matches"
    }
}
Check $allMatch 'all 5 replicas converged on an identical committed prefix'

# ---------------------------------------------------------------- phase 9

Write-Header 'Phase 9: no command was lost'

$expected = @(
    "$TestPrefix-before-1", "$TestPrefix-before-2", "$TestPrefix-before-3",
    "$TestPrefix-after-1",  "$TestPrefix-after-2"
)

$finalLog = Get-ReplicaLog $Ports[0]
$values = @()
if ($null -ne $finalLog -and $null -ne $finalLog.log) {
    $values = @($finalLog.log | ForEach-Object { $_.command.value } | Where-Object { $_ })
}

foreach ($v in $expected) {
    Check ($values -contains $v) "committed log contains '$v'"
}

Write-Host ''
Write-Host '  Committed entries:'
if ($null -ne $finalLog -and $null -ne $finalLog.log) {
    $finalLog.log | ForEach-Object {
        $val = if ($_.command.value) { $_.command.value } else { "($($_.command.type))" }
        Write-Host "    [$($_.index)] term=$($_.term) $val"
    }
}

# ---------------------------------------------------------------- summary

Write-Host ''
if ($Failures -eq 0) {
    Write-Host '==========================================' -ForegroundColor Green
    Write-Host ' FAILURE / RECOVERY TEST PASSED'            -ForegroundColor Green
    Write-Host ' Survived 2 simultaneous node failures'     -ForegroundColor Green
    Write-Host ' including loss of the leader.'             -ForegroundColor Green
    Write-Host '==========================================' -ForegroundColor Green
    exit 0
} else {
    Write-Host '==========================================' -ForegroundColor Red
    Write-Host " FAILED - $Failures check(s) did not pass"  -ForegroundColor Red
    Write-Host '==========================================' -ForegroundColor Red
    Write-Host ''
    Write-Host 'Recent logs:'
    & docker @Compose logs --tail=40
    exit 1
}