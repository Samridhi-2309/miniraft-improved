#!/bin/bash
set -uo pipefail

COMPOSE="docker compose -f docker-compose.5node.yml"

REPLICAS=(replica1 replica2 replica3 replica4 replica5)
PORTS=(4001 4002 4003 4004 4005)

TEST_PREFIX="failure-test"

# ============================================================
# Helper: get replica state
# ============================================================

get_state() {
    local port="$1"

    curl -s --fail "http://localhost:${port}/state"
}

# ============================================================
# Helper: find current leader
# ============================================================

find_leader() {
    for i in "${!PORTS[@]}"; do
        local port="${PORTS[$i]}"
        local state
        local role

        state=$(get_state "$port" 2>/dev/null) || continue

        role=$(echo "$state" | jq -r '.role // "dead"')

        if [ "$role" = "leader" ]; then
            echo "$i"
            return 0
        fi
    done

    return 1
}

# ============================================================
# Helper: send command to a leader
# ============================================================

send_command() {
    local leader_index="$1"
    local value="$2"
    local port="${PORTS[$leader_index]}"

    echo "Sending '$value' to replica$((leader_index + 1))..."

    local response

    response=$(curl -s --fail \
        -X POST "http://localhost:${port}/command" \
        -H "Content-Type: application/json" \
        -d "{\"command\":{\"type\":\"test\",\"value\":\"${value}\"}}") || {
        echo "ERROR: command request failed"
        return 1
    }

    echo "$response"

    local ok
    ok=$(echo "$response" | jq -r '.ok // false')

    if [ "$ok" != "true" ]; then
        echo "ERROR: command was not accepted"
        return 1
    fi

    return 0
}

# ============================================================
# Helper: get complete log
# Used for human-readable debugging/output.
# ============================================================

get_log() {
    local port="$1"

    curl -s --fail "http://localhost:${port}/log"
}

# ============================================================
# Helper: get committed prefix only
#
# Raft guarantees that committed entries are identical across
# replicas. Uncommitted log tails may temporarily differ.
# ============================================================

get_committed() {
    local port="$1"

    curl -s --fail "http://localhost:${port}/log" | jq -cS '.committed'
}

# ============================================================
# STEP 0: Reset persistent state
# ============================================================

echo "=========================================="
echo "5-NODE RAFT FAILURE/RECOVERY TEST"
echo "=========================================="

echo
echo "=== Resetting cluster ==="

$COMPOSE down

rm -rf \
    ./data/replica1 \
    ./data/replica2 \
    ./data/replica3 \
    ./data/replica4 \
    ./data/replica5

mkdir -p ./data/replica1
mkdir -p ./data/replica2
mkdir -p ./data/replica3
mkdir -p ./data/replica4
mkdir -p ./data/replica5

echo "Persistent replica data cleared."

# ============================================================
# STEP 1: Start cluster
# ============================================================

echo
echo "=== Starting 5-node cluster ==="

$COMPOSE up -d

echo
echo "=== Waiting for cluster to start ==="

sleep 5

echo
echo "=== Initial cluster state ==="

$COMPOSE ps

# ============================================================
# STEP 2: Discover initial leader
# ============================================================

echo
echo "=== Discovering current leader ==="

LEADER_INDEX=-1

for attempt in {1..15}; do
    LEADER_INDEX=$(find_leader 2>/dev/null) || LEADER_INDEX=-1

    if [ "$LEADER_INDEX" -ge 0 ]; then
        break
    fi

    echo "No leader yet, waiting..."
    sleep 1
done

if [ "$LEADER_INDEX" -lt 0 ]; then
    echo "ERROR: No leader found."
    exit 1
fi

LEADER_REPLICA=$((LEADER_INDEX + 1))

echo "Leader is replica${LEADER_REPLICA}"

# ============================================================
# STEP 3: Create commands before failure
# ============================================================

echo
echo "=== Creating pre-failure commands ==="

send_command "$LEADER_INDEX" "${TEST_PREFIX}-before-1" || exit 1
send_command "$LEADER_INDEX" "${TEST_PREFIX}-before-2" || exit 1
send_command "$LEADER_INDEX" "${TEST_PREFIX}-before-3" || exit 1

echo
echo "=== Waiting for replication and commit ==="

sleep 3

# ============================================================
# STEP 4: Verify committed prefix before failure
# ============================================================

echo
echo "=== Verifying committed logs before failure ==="

REFERENCE_COMMITTED=$(get_committed "${PORTS[0]}") || {
    echo "ERROR: Could not read replica1 committed log."
    exit 1
}

for i in "${!PORTS[@]}"; do
    COMMITTED=$(get_committed "${PORTS[$i]}") || {
        echo "ERROR: Could not read replica$((i + 1)) committed log."
        exit 1
    }

    if [ "$COMMITTED" != "$REFERENCE_COMMITTED" ]; then
        echo
        echo "ERROR: Replica$((i + 1)) committed log differs before failure."
        exit 1
    fi

    echo "Replica$((i + 1)): committed log matches."
done

echo
echo "All 5 replicas have identical committed logs."

# ============================================================
# STEP 5: Kill current leader + one follower
# ============================================================

FOLLOWER_INDEX=$(( (LEADER_INDEX + 1) % 5 ))

echo
echo "=== Killing leader and one follower ==="

echo "Killing replica$((LEADER_INDEX + 1))"
echo "Killing replica$((FOLLOWER_INDEX + 1))"

docker kill --signal=SIGKILL \
    "replica$((LEADER_INDEX + 1))" \
    "replica$((FOLLOWER_INDEX + 1))"

sleep 3

echo
echo "=== Cluster after failure ==="

$COMPOSE ps -a

# ============================================================
# STEP 6: Find new leader among surviving 3 replicas
# ============================================================

echo
echo "=== Waiting for new leader among surviving replicas ==="

NEW_LEADER_INDEX=-1

for attempt in {1..15}; do

    NEW_LEADER_INDEX=$(find_leader 2>/dev/null) || NEW_LEADER_INDEX=-1

    if [ "$NEW_LEADER_INDEX" -ge 0 ]; then

        # Make sure the leader is one of the surviving replicas.
        if [ "$NEW_LEADER_INDEX" -ne "$LEADER_INDEX" ] &&
           [ "$NEW_LEADER_INDEX" -ne "$FOLLOWER_INDEX" ]; then
            break
        fi
    fi

    NEW_LEADER_INDEX=-1

    echo "Waiting for surviving majority to elect a leader..."

    sleep 1
done

if [ "$NEW_LEADER_INDEX" -lt 0 ]; then
    echo
    echo "ERROR: No new leader elected among surviving 3 replicas."
    exit 1
fi

echo
echo "New leader is replica$((NEW_LEADER_INDEX + 1))"

# ============================================================
# STEP 7: Send commands through surviving majority
# ============================================================

echo
echo "=== Sending commands after failure ==="

send_command \
    "$NEW_LEADER_INDEX" \
    "${TEST_PREFIX}-after-1" || exit 1

send_command \
    "$NEW_LEADER_INDEX" \
    "${TEST_PREFIX}-after-2" || exit 1

echo
echo "=== Waiting for replication and commit ==="

sleep 3

# ============================================================
# STEP 8: Verify surviving replicas contain commands
# ============================================================

echo
echo "=== Verifying surviving replicas ==="

for i in "${!PORTS[@]}"; do

    # Skip the two killed replicas.
    if [ "$i" -eq "$LEADER_INDEX" ] ||
       [ "$i" -eq "$FOLLOWER_INDEX" ]; then
        continue
    fi

    LOG=$(get_log "${PORTS[$i]}") || {
        echo "ERROR: Could not read surviving replica$((i + 1)) log."
        exit 1
    }

    echo
    echo "Replica$((i + 1)) log:"

    echo "$LOG" | jq -r \
        '.log[] |
        "\(.index): term=\(.term) command=\(.command.type) value=\(.command.value // "")"'

    if ! echo "$LOG" | jq -e \
        --arg value "${TEST_PREFIX}-after-2" \
        '.log[] | select(.command.value == $value)' \
        >/dev/null; then

        echo
        echo "ERROR: Replica$((i + 1)) does not contain ${TEST_PREFIX}-after-2."
        exit 1
    fi

done

echo
echo "Surviving majority contains the post-failure commands."

# ============================================================
# STEP 9: Restart failed replicas
# ============================================================

echo
echo "=== Restarting failed replicas ==="

$COMPOSE up -d \
    "replica$((LEADER_INDEX + 1))" \
    "replica$((FOLLOWER_INDEX + 1))"

echo
echo "=== Waiting for failed replicas to recover ==="

sleep 5

echo
echo "=== Cluster after recovery ==="

$COMPOSE ps

# ============================================================
# STEP 10: Verify exactly one leader
# ============================================================

echo
echo "=== Verifying exactly one leader ==="

LEADER_COUNT=0
RECOVERED_LEADER=-1

for i in "${!PORTS[@]}"; do

    role=$(get_state "${PORTS[$i]}" 2>/dev/null |
        jq -r '.role // "dead"')

    echo "Replica$((i + 1)): $role"

    if [ "$role" = "leader" ]; then
        LEADER_COUNT=$((LEADER_COUNT + 1))
        RECOVERED_LEADER="$i"
    fi

done

if [ "$LEADER_COUNT" -ne 1 ]; then
    echo
    echo "ERROR: $LEADER_COUNT leaders found."
    echo "Expected exactly 1 leader."
    exit 1
fi

echo
echo "Exactly one leader is present."

# ============================================================
# STEP 11: Verify recovered committed logs
# ============================================================

echo
echo "=== Verifying recovered committed logs ==="

REFERENCE_COMMITTED=$(get_committed "${PORTS[0]}") || {
    echo "ERROR: Could not read replica1 committed log after recovery."
    exit 1
}

for i in "${!PORTS[@]}"; do

    COMMITTED=$(get_committed "${PORTS[$i]}") || {
        echo "ERROR: Could not read replica$((i + 1)) committed log after recovery."
        exit 1
    }

    if [ "$COMMITTED" != "$REFERENCE_COMMITTED" ]; then

        echo
        echo "ERROR: Replica$((i + 1)) committed log differs after recovery."

        echo
        echo "Replica 1 committed log:"
        echo "$REFERENCE_COMMITTED" | jq .

        echo
        echo "Replica$((i + 1)) committed log:"
        echo "$COMMITTED" | jq .

        exit 1
    fi

    echo "Replica$((i + 1)): committed log matches."

done

echo
echo "ALL 5 REPLICAS HAVE IDENTICAL COMMITTED LOGS AFTER RECOVERY."

# ============================================================
# STEP 12: Verify important commands survived
# ============================================================

echo
echo "=== Verifying command history ==="

FINAL_COMMITTED=$(get_committed "${PORTS[0]}") || {
    echo "ERROR: Could not read final committed log."
    exit 1
}

for VALUE in \
    "${TEST_PREFIX}-before-1" \
    "${TEST_PREFIX}-before-2" \
    "${TEST_PREFIX}-before-3" \
    "${TEST_PREFIX}-after-1" \
    "${TEST_PREFIX}-after-2"
do

    if ! echo "$FINAL_COMMITTED" | jq -e \
        --arg value "$VALUE" \
        '.[] | select(.command.value == $value)' \
        >/dev/null; then

        echo
        echo "ERROR: Missing command from final committed log:"
        echo "$VALUE"

        exit 1
    fi

done

echo
echo "All expected commands are present."

echo
echo "=== Final committed log ==="

echo "$FINAL_COMMITTED" | jq -r \
    '.[] |
    if .command.value then
        "\(.index): term=\(.term) value=\(.command.value)"
    else
        "\(.index): term=\(.term) command=\(.command.type)"
    end'

# ============================================================
# FINAL RESULT
# ============================================================

echo
echo "=========================================="
echo "FAILURE/RECOVERY TEST PASSED"
echo "=========================================="

echo
echo "Verified:"
echo "  - 5-node cluster started"
echo "  - Initial leader elected"
echo "  - Commands replicated before failure"
echo "  - Committed prefix identical before failure"
echo "  - Leader + follower killed simultaneously"
echo "  - Surviving 3-node majority elected a new leader"
echo "  - Commands committed during failure"
echo "  - Failed replicas recovered"
echo "  - Exactly one leader after recovery"
echo "  - Committed prefix identical across all 5 replicas"
echo "  - Pre-failure commands preserved"
echo "  - Post-failure commands preserved"
echo