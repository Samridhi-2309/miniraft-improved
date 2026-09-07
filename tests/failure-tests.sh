#!/bin/bash
set -e

COMPOSE="docker compose -f docker-compose.5node.yml"
LEADER="http://localhost:4001"
TEST_VALUE="failure-recovery-test"

echo "=== Starting 5-node cluster ==="
$COMPOSE up -d
sleep 3

echo
echo "=== Initial cluster state ==="
$COMPOSE ps

echo
echo "=== Killing replica4 and replica5 ==="
docker kill --signal=SIGKILL replica4 replica5
sleep 2

echo
echo "=== Verifying 3/5 replicas remain alive ==="
$COMPOSE ps

echo
echo "=== Sending command through surviving leader ==="
curl -s -X POST "$LEADER/command" \
  -H "Content-Type: application/json" \
  -d "{\"command\":{\"type\":\"test\",\"value\":\"$TEST_VALUE\"}}"

echo
echo
echo "=== Waiting for replication/commit ==="
sleep 2

echo
echo "=== Surviving replicas' logs ==="
$COMPOSE logs --tail=30 replica1 replica2 replica3

echo
echo "=== Restarting replica4 and replica5 ==="
$COMPOSE up -d replica4 replica5
sleep 3

echo
echo "=== Verifying recovered replicas ==="
$COMPOSE ps

echo
echo "=== Recovery logs ==="
$COMPOSE logs --tail=30 replica4 replica5

echo
echo "=== Failure/recovery test complete ==="