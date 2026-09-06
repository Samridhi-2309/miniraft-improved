const { quorumSize, RPC_TIMEOUT } = require('./constants');

class ReplicationManager {
  constructor(state, peers, logger, broadcastFn) {
    this.state = state;
    this.peers = peers;
    this.logger = logger;
    this.broadcastFn = broadcastFn || (() => {}); // callback to broadcast strokes to clients

    // Majority quorum = majority of all nodes, including this node
    this.quorum = quorumSize(peers.length + 1);

    // nextIndex: index of next log entry to send to each follower
    // matchIndex: index of highest log entry known to be replicated on follower
    this.nextIndex = {};
    this.matchIndex = {};

    this.resetForNewLeader();
  }

  resetForNewLeader() {
    const nextIdx = this.state.getLogLength();

    for (const peer of this.peers) {
      this.nextIndex[peer] = nextIdx;
      this.matchIndex[peer] = -1; // -1 means no entries replicated yet
    }
  }

  async replicateToAll() {
    // Called by leader heartbeat loop
    if (!this.state.isLeader()) {
      return;
    }

    for (const peerUrl of this.peers) {
      this.replicateToPeer(peerUrl).catch((err) => {
        this.logger.debug(
          `replicateToPeer failed ${peerUrl}: ${err.message}`
        );
      });
    }

    // After sending replication attempts, try commit
    this.tryAdvanceCommitIndex();
    this.applyCommittedEntries();
  }

  async replicateToPeer(peerUrl) {
    if (!this.state.isLeader()) {
      return;
    }

    const nextIdx = this.nextIndex[peerUrl];
    const prevLogIndex = nextIdx - 1;
    const prevLogTerm =
      prevLogIndex >= 0
        ? this.state.getEntryAt(prevLogIndex)?.term || 0
        : 0;

    const entries = this.state.getEntriesFrom(nextIdx);

    const payload = {
      term: this.state.currentTerm,
      leaderId: this.state.replicaId,
      entries,
      prevLogIndex,
      prevLogTerm,
      leaderCommit: this.state.commitIndex
    };

    try {
      const response = await Promise.race([
        fetch(`${peerUrl}/rpc/append-entries`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        }),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('timeout')), RPC_TIMEOUT)
        )
      ]);

      if (!response.ok) {
        this.logger.debug(
          `Append entries to ${peerUrl} returned ${response.status}`
        );
        return;
      }

      const result = await response.json();

      if (result.term > this.state.currentTerm) {
        // higher term seen; follower is behind, election manager handles this elsewhere
        this.logger.info(
          `Higher term detected ${result.term} from ${peerUrl}`
        );
        return;
      }

      if (result.success) {
        this.matchIndex[peerUrl] = nextIdx + entries.length - 1;
        this.nextIndex[peerUrl] = this.matchIndex[peerUrl] + 1;

        this.logger.rpc(
          'SEND',
          'append-entries',
          'success',
          `peer=${peerUrl} ${this.matchIndex[peerUrl]}`
        );
      } else {
        // AppendEntries failed: back up nextIndex and retry later.
        // The follower will detect conflicting entries and truncate
        // its log through the normal AppendEntries mechanism.
        this.nextIndex[peerUrl] = Math.max(
          0,
          this.nextIndex[peerUrl] - 1
        );

        this.logger.rpc(
          'SEND',
          'append-entries',
          'failure',
          `peer=${peerUrl} decrease nextIndex`
        );
      }
    } catch (err) {
      this.logger.debug(
        `append-entries request ${peerUrl} failed: ${err.message}`
      );
    }
  }

  tryAdvanceCommitIndex() {
    if (!this.state.isLeader()) {
      return;
    }

    const N = this.state.getLogLength() - 1;

    // Find the highest index reachable at the current term, then commit everything up to it.
    // Per RAFT §5.4.2: a leader only directly commits entries from its current term;
    // prior-term entries are committed indirectly when a current-term entry is committed.
    let highestCommittable = -1;

    for (let idx = this.state.commitIndex + 1; idx <= N; idx++) {
      const replicatedCount =
        Object.values(this.matchIndex).filter((m) => m >= idx).length + 1; // +1 for leader itself

      if (replicatedCount >= this.quorum) {
        const entry = this.state.getEntryAt(idx);

        if (entry && entry.term === this.state.currentTerm) {
          highestCommittable = idx;
        }
      }
    }

    // Advance commitIndex to highestCommittable — this also commits all prior-term
    // entries between the old commitIndex and highestCommittable (RAFT §5.4.2).
    if (highestCommittable > this.state.commitIndex) {
      this.state.updateCommitIndex(highestCommittable);
      this.logger.info(`commitIndex advanced to ${highestCommittable}`);
    }
  }

  /**
   * Append a no-op entry at the current term.
   * Called by the leader immediately after winning an election so that prior-term
   * log entries (which cannot be committed directly) get committed indirectly once
   * this no-op reaches quorum. (RAFT §8 / leader completeness)
   */
  commitNoOp() {
    if (!this.state.isLeader()) {
      return;
    }

    this.state.appendEntry({
      term: this.state.currentTerm,
      command: { type: 'no-op' }
    });

    this.logger.info(
      `[NO-OP] Appended no-op entry at term ${this.state.currentTerm} to unblock prior-term commits`
    );
  }

  applyCommittedEntries() {
    while (this.state.lastApplied < this.state.commitIndex) {
      this.state.lastApplied += 1;

      const logEntry = this.state.getEntryAt(this.state.lastApplied);

      if (logEntry) {
        this.logger.info(
          `Applying log entry ${this.state.lastApplied}: ${JSON.stringify(logEntry)}`
        );

        if (logEntry.command && logEntry.command.type === 'stroke') {
          // Send committed stroke to connected clients via callback
          try {
            const body = Object.assign({}, logEntry.command, {
              index: this.state.lastApplied,
              term: this.state.currentTerm,
              replicaId: this.state.replicaId
            });

            // Execute the callback synchronously or asynchronously (doesn't matter)
            this.broadcastFn(body);

            // In RAFT, followers apply logs through AppendEntries,
            // so they will broadcast locally when they advance commitIndex.
          } catch (err) {
            this.logger.warn(
              `Failed to broadcast stroke: ${err.message}`
            );
          }
        }
      }
    }
  }
}

module.exports = ReplicationManager;