/**
 * RAFT State Manager
 * 
 * Encapsulates the persistent and volatile state of a RAFT node.
 * Provides state transitions, validation, and helper methods.
 * 
 * Persistent State:
 * - currentTerm: Latest term server has seen
 * - votedFor: CandidateId that received vote in current term
 * - log: Append-only log of entries
 * 
 * Volatile State (Lost on restart):
 * - commitIndex: Index of highest log entry known to be committed
 * - lastApplied: Index of highest log entry applied to state machine
 * - role: Current node role (follower, candidate, leader)
 * - leaderId: Current leader's ID (if known)
 */

const fs = require('fs');
const path = require('path');

class RaftState {
  constructor(replicaId) {
    this.replicaId = replicaId;

    // Persistence path (can be overridden via env)
    const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data');
    if (!fs.existsSync(dataDir)) {
      try { fs.mkdirSync(dataDir, { recursive: true }); } catch (e) { /* ignore */ }
    }
    this._persistPath = path.join(dataDir, `raft-${replicaId}.json`);

    // Persistent state (loaded from disk when available)
    this.currentTerm = 0;
    this.votedFor = null;
    this.log = [];
    this.processedCommandIds = new Set();

    // Volatile state
    this.commitIndex = -1;
    this.lastApplied = -1;
    this.role = 'follower'; // 'follower', 'candidate', 'leader'
    this.leaderId = null;

    // Metrics for logging/debugging
    this.stateChangeTimestamp = Date.now();

    // Load persisted state if present
    this._loadFromDisk();
    this._resetRuntimeState();
  }

  _resetRuntimeState() {
    // Only reset VOLATILE state (role, leaderId).
    // Persistent state (currentTerm, votedFor, log) is loaded from disk in _loadFromDisk().
    this.role = 'follower';
    this.leaderId = null;
    this.stateChangeTimestamp = Date.now();
  }

  /**
   * Transition to follower state
   * Called when:
   * - Node starts up
   * - Receives message with higher term
   * - Times out during election
   */
  toFollower(term = this.currentTerm) {
    const termChanged = term > this.currentTerm;
    const changed = this.role !== 'follower' || termChanged;

    if (termChanged) {
      this.currentTerm = term;
      this.votedFor = null;
    }

    this.role = 'follower';
    this.leaderId = null;

    if (changed) {
      this.stateChangeTimestamp = Date.now();
    }

    return changed;
  }

  /**
   * Transition to candidate state
   * Called when:
   * - Election timeout fires
   * - Follower decides to start an election
   */
  toCandidate() {
    this.currentTerm += 1;
    this.role = 'candidate';
    this.votedFor = this.replicaId; // Vote for self
    this.leaderId = null;
    this.stateChangeTimestamp = Date.now();
    this._saveToDisk();
    return this.currentTerm;
  }

  /**
   * Transition to leader state
   * Called when:
   * - Candidate receives majority votes
   */
  toLeader() {
    const changed = this.role !== 'leader';
    this.role = 'leader';
    this.leaderId = this.replicaId;
    if (changed) {
      this.stateChangeTimestamp = Date.now();
    }
    return changed;
  }

  /**
   * Update term if higher term is seen
   * Returns true if term was updated (node should revert to follower)
   */
  updateTerm(term) {
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.role = 'follower';
      this.leaderId = null;
      this.stateChangeTimestamp = Date.now();
      this._saveToDisk();
      return true;
    }
    return false;
  }

  /**
   * Record a vote for the given candidate in current term
   * Returns true if vote was accepted, false if already voted for someone else
   */
  vote(candidateId) {
    if (this.votedFor === null || this.votedFor === candidateId) {
      this.votedFor = candidateId;
      this._saveToDisk();
      return true;
    }
    return false;
  }

  /**
   * Check whether a command has already been processed.
   */
  hasProcessedCommand(commandId) {
    return Boolean(commandId) && this.processedCommandIds.has(commandId);
  }

  /**
   * Mark a command as processed.
   */
  markCommandProcessed(commandId) {
    if (commandId) {
      this.processedCommandIds.add(commandId);
    }
  }

  /**
   * Append entry to log
   * Used by both followers (receiving entries) and leaders (adding new entries)
   */
  appendEntry(entry) {
    const index = this.log.length;
    this.log.push({
      ...entry,
      index,
      term: entry.term || this.currentTerm
    });
    this._saveToDisk();
    return index;
  }

  /**
   * Append multiple entries to log
   * Used during catch-up or bulk replication
   */
  appendEntries(entries) {
    const startIndex = this.log.length;
    const indices = entries.map((entry, i) => {
      this.log.push({
        ...entry,
        index: startIndex + i,
        term: entry.term || this.currentTerm
      });
      return startIndex + i;
    });
    this._saveToDisk();
    return indices;
  }

  /**
   * Get entry at specific index
   */
  getEntryAt(index) {
    if (index < 0 || index >= this.log.length) {
      return null;
    }
    return this.log[index];
  }

  /**
   * Get all entries from index onwards
   */
  getEntriesFrom(index) {
    if (index < 0) return [];
    return this.log.slice(index);
  }

  /**
   * Get log length
   */
  getLogLength() {
    return this.log.length;
  }

  /**
   * Get last log index and term
   * Used in RequestVote RPC
   */
  getLastLogIndexAndTerm() {
    if (this.log.length === 0) {
      return { lastLogIndex: -1, lastLogTerm: 0 };
    }
    const lastEntry = this.log[this.log.length - 1];
    return {
      lastLogIndex: lastEntry.index,
      lastLogTerm: lastEntry.term
    };
  }

  /**
   * Update commit index
   * Leader uses this to advance commitIndex and followers follow leader's commitIndex
   */
  updateCommitIndex(newCommitIndex) {
    const oldCommitIndex = this.commitIndex;
    if (newCommitIndex > this.commitIndex && newCommitIndex < this.getLogLength()) {
      this.commitIndex = newCommitIndex;
      return true;
    }
    return false;
  }

  /**
   * Persist current persistent state to disk (synchronous safe writes)
   */
  _saveToDisk() {
    try {
      const payload = {
        currentTerm: this.currentTerm,
        votedFor: this.votedFor,
        log: this.log
      };
      const tempPath = `${this._persistPath}.tmp`;
      fs.writeFileSync(tempPath, JSON.stringify(payload), { encoding: 'utf8' });
      fs.renameSync(tempPath, this._persistPath);
    } catch (err) {
      // swallow persistence errors but log if environment requests it
      if (process.env.DEBUG) console.warn(`[RaftState] save failed: ${err.message}`);
    }
  }

  _loadFromDisk() {
    try {
      if (!fs.existsSync(this._persistPath)) return;
      const raw = fs.readFileSync(this._persistPath, { encoding: 'utf8' });
      const obj = JSON.parse(raw || '{}');
      // Restore ALL persistent state (term + votedFor + log) as required by the RAFT spec.
      if (typeof obj.currentTerm === 'number' && obj.currentTerm >= 0) {
        this.currentTerm = obj.currentTerm;
      }
      if (obj.votedFor !== undefined) {
        this.votedFor = obj.votedFor;
      }
      if (Array.isArray(obj.log)) this.log = obj.log;
    } catch (err) {
      if (process.env.DEBUG) console.warn(`[RaftState] load failed: ${err.message}`);
    }
  }

  /**
   * Get current state snapshot for diagnostics/logging
   */
  getSnapshot() {
    return {
      replicaId: this.replicaId,
      currentTerm: this.currentTerm,
      votedFor: this.votedFor,
      role: this.role,
      leaderId: this.leaderId,
      logLength: this.log.length,
      commitIndex: this.commitIndex,
      lastApplied: this.lastApplied,
      uptime: Date.now() - this.stateChangeTimestamp
    };
  }

  /**
   * Check if node is leader
   */
  isLeader() {
    return this.role === 'leader';
  }

  /**
   * Check if node is follower
   */
  isFollower() {
    return this.role === 'follower';
  }

  /**
   * Check if node is candidate
   */
  isCandidate() {
    return this.role === 'candidate';
  }

  /**
   * Validate that persistent state is consistent
   */
  validate() {
    const errors = [];
    
    if (typeof this.currentTerm !== 'number' || this.currentTerm < 0) {
      errors.push('currentTerm must be non-negative number');
    }
    
    if (this.votedFor !== null && typeof this.votedFor !== 'string') {
      errors.push('votedFor must be null or string');
    }
    
    if (!Array.isArray(this.log)) {
      errors.push('log must be array');
    }
    
    if (!['follower', 'candidate', 'leader'].includes(this.role)) {
      errors.push('role must be follower, candidate, or leader');
    }
    
    return {
      valid: errors.length === 0,
      errors
    };
  }
}

module.exports = RaftState;
