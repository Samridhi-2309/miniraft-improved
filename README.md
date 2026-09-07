# DoodleDock (MiniRaft)

Real-time collaborative drawing backed by a **RAFT consensus cluster implemented from scratch** — leader election, log replication and state synchronisation, with no consensus library.

Strokes drawn in the browser are replicated through a five-node RAFT cluster and only broadcast back to clients once committed by a majority. The cluster tolerates two simultaneous node failures and network partitions without losing committed data.

<img width="1470" height="808" alt="DoodleDock canvas" src="https://github.com/user-attachments/assets/678f9979-af6e-4a6d-a805-2d4388c78d68" />

---

## What this is

Most collaborative canvases broadcast strokes optimistically and hope for the best. This one runs them through consensus first, which makes the failure behaviour precise and testable:

- **Leader election** with randomized timeouts, term-based voting, and the election restriction (a candidate wins only if its log is at least as up-to-date as the voter's)
- **Log replication** with the AppendEntries consistency check, `nextIndex` back-off on rejection, and conflict-only truncation
- **Commit safety** per Raft §5.4.2 — the leader only counts replicas for entries in its own term, and appends a no-op on election so prior-term entries commit indirectly
- **Crash recovery** — `currentTerm`, `votedFor` and the log are fsync'd to disk before any RPC is answered
- **Client deduplication** — commands carry a `commandId`, so a gateway retry after a timeout cannot apply the same stroke twice

Every claim above has a test behind it. See [Testing](#testing).

---

## Architecture

```
Browser ──WebSocket──> Gateway ──HTTP POST /command──> Leader replica
                          ▲                                 │
                          │                        HTTP /rpc/append-entries
                          │                                 ▼
                          └────HTTP POST /commit──── Follower replicas
```

**Transport, precisely:** RAFT RPCs between replicas are **HTTP POST**. WebSockets are used **only** between the gateway and the browser. The gateway is stateless routing — it discovers the current leader, forwards commands to it, and broadcasts committed strokes to connected clients.

### Cluster sizing

Quorum is `⌊N/2⌋ + 1`, derived from the peer list at runtime — never hardcoded.

| Nodes | Quorum | Tolerates |
|---|---|---|
| 3 | 2 | 1 failure |
| **5** | **3** | **2 failures** |

`docker-compose.5node.yml` is the primary configuration. `docker-compose.yml` runs a 3-node cluster for quicker local iteration.

### Protocol timing

| Constant | Value |
|---|---|
| `HEARTBEAT_INTERVAL` | 150 ms |
| `ELECTION_TIMEOUT` | 1500–3000 ms (randomized) |
| `RPC_TIMEOUT` | 500 ms |

The invariant that must hold is `HEARTBEAT_INTERVAL < RPC_TIMEOUT < ELECTION_TIMEOUT_MIN`. Randomizing the election timeout across a 1500 ms spread is what prevents repeated split votes.

---

## Quickstart

Prerequisites: Docker and Docker Compose.

```bash
# 5-node cluster (recommended)
docker compose -f docker-compose.5node.yml up --build -d

# or the 3-node cluster
make setup && make up
```

Open **http://localhost:3000**.

```bash
# tear down, including persisted RAFT state
docker compose -f docker-compose.5node.yml down -v
```

Replica state lives in **named Docker volumes**, not host bind mounts. This matters: `fsync` on a bind mount through the Docker Desktop VM layer can block Node's event loop for longer than `RPC_TIMEOUT`, which starves vote responses and livelocks elections. Named volumes sit on the VM's native filesystem, where fsync is fast.

### Running components directly

```bash
# gateway
REPLICA_ENDPOINTS="http://localhost:4001,http://localhost:4002,http://localhost:4003,http://localhost:4004,http://localhost:4005" \
PORT=3000 npm run start:gateway

# one replica
REPLICA_ID=1 PORT=4001 \
PEERS="http://localhost:4002,http://localhost:4003,http://localhost:4004,http://localhost:4005" \
npm run start:replica
```

---

## Testing

Two scripts, both of which assert and exit non-zero on failure rather than printing logs for a human to read.

### Failure recovery — surviving two simultaneous crashes

```powershell
powershell -ExecutionPolicy Bypass -File tests\failure-tests.ps1
```

Kills the **leader plus one follower** with `SIGKILL` — the hard case, since it forces a re-election with only three of five nodes remaining. Verifies:

1. All five replicas agree on the committed prefix before the failure
2. The surviving three elect a new leader
3. The cluster still accepts and commits writes with two nodes down
4. Killed nodes restart and catch up from persisted state
5. Exactly one leader afterwards
6. All five committed prefixes are identical, with no command lost

`SIGKILL` rather than `SIGTERM` is deliberate — a graceful shutdown lets the process flush state and doesn't test crash recovery.

### Network partition — no split-brain

```powershell
powershell -ExecutionPolicy Bypass -File tests\partition-test.ps1
```

Uses `docker network disconnect` to isolate two nodes (including the current leader) from the other three. A killed node stops; a **partitioned** leader keeps running and still believes it leads, which is where split-brain becomes possible. Verifies:

1. The majority side elects a new leader at a strictly higher term
2. The majority side can still commit
3. After healing, exactly one leader remains — the old leader steps down
4. All five replicas converge on an identical committed prefix
5. **The write attempted on the minority side is not in the committed log**

A bash equivalent (`tests/failure-tests.sh`) is available for Linux and macOS; it requires `jq`.

### Why the *committed prefix*, not the whole log

RAFT does not guarantee that all replicas hold identical logs at any instant — a follower can legitimately lag by an uncommitted entry. What it guarantees is that the **committed prefix** is identical everywhere. Comparing full logs produces false failures on a correct system, so the tests compare `log[0..commitIndex]`.

---

## API

### Gateway

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/leader` | Currently known leader URL |
| `GET` | `/cluster` | Configured replica endpoints |
| `GET` | `/clients` | Connected WebSocket client count |
| `POST` | `/commit` | Leader posts committed entries here for broadcast |

WebSocket clients send `{ type: 'stroke', points, color, timestamp }` and receive committed strokes in the same shape. When no leader is reachable the gateway replies `{ type: 'queued' }`.

### Replica

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Health and basic state |
| `GET` | `/state` | Role, term, `commitIndex`, `logLength` |
| `GET` | `/log` | Full log plus the committed prefix |
| `POST` | `/command` | Client write path — rejected with 400 unless leader |
| `POST` | `/rpc/request-vote` | RAFT election RPC |
| `POST` | `/rpc/append-entries` | RAFT replication RPC |

---

## Project layout

```
src/
├── gateway/            HTTP + WebSocket server, leader routing
├── replica/            Replica HTTP API and RPC handlers
├── replicas/common/    RAFT internals
│   ├── raftState.js        persistent + volatile state, fsync'd persistence
│   ├── election.js         candidate state machine, vote tracking
│   ├── electionTimeout.js  randomized timeout
│   ├── replicationManager.js  AppendEntries, commit advancement
│   └── constants.js        protocol timing, quorum derivation
└── frontend/           Canvas UI
tests/                  Failure and partition validation
infra/docker/           Dockerfiles
```

---

## Known limitations

Stated deliberately rather than left to be discovered.

**No log compaction or snapshotting.** The log grows without bound — every stroke is an entry forever, and a new replica replays the whole history to catch up. The fix is per-server snapshots with `lastIncludedIndex`/`lastIncludedTerm` and an `InstallSnapshot` RPC.

**No PreVote.** During a partition, an isolated minority repeatedly starts elections it cannot win, inflating its term each time. On reconnect that higher term unseats a perfectly healthy leader. The partition test reproduces this — the minority advanced roughly ten terms in thirty seconds. PreVote (Raft dissertation §9.6) fixes it: a candidate runs a preliminary round before incrementing its term, so an isolated node rejoins quietly.

**The gateway is a single point of failure.** The state machine is replicated; the process in front of it is not. It's stateless routing apart from an in-memory queue for strokes received while no leader is reachable — those strokes are lost if the gateway dies. Note the scope of the durability claim: RAFT guarantees no loss of **committed** entries, and queued strokes were never committed.

**No ReadIndex or leader leases.** A partitioned leader doesn't know it has been deposed and keeps reporting `role: 'leader'`, so reads from it can be stale. Writes fail safely, since it cannot reach a quorum.

**Persistence rewrites the whole log as JSON on every append** — O(n) per append. Real implementations use segmented append-only log files.

**Testing is scripted fault injection**, not linearizability checking. Jepsen-style verification against a model would be the next step.

---

## Development notes

- Replica images are built from a single shared `./src` mount; nodes differ only by `REPLICA_ID`, `PORT` and `PEERS`.
- `restart: "no"` is set deliberately in the 5-node compose. A restart policy would resurrect nodes killed during a fault-tolerance test and silently hide crash bugs.
- `make logs`, `make ps`, `make health` are available for the 3-node setup.