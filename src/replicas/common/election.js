/**
 * Election Logic Module
 * 
 * Handles RAFT leader election process:
 * 1. Election timeout fires → follower becomes candidate
 * 2. Candidate increments term and requests votes from all peers
 * 3. Candidate collects responses
 * 4. If majority votes received → becomes leader
 * 5. If higher term received or timeout → returns to follower
 * 
 * This module does NOT handle the actual HTTP RPC calls (that's in server.js)
 * It focuses on the election state machine and vote tracking.
 */

const { quorumSize } = require('./constants');

class ElectionManager {
  constructor(state, peers, logger) {
    this.state = state;
    this.peers = peers; // Array of peer URLs
    this.logger = logger;

    // Majority quorum = majority of all nodes, including this node
    this.quorum = quorumSize(peers.length + 1);

    // Vote tracking during election
    this.votesReceived = new Set(); // Set of replica IDs that voted for us
    this.electionInProgress = false;
    this.currentElectionTerm = null;
  }

  /**
   * Start an election
   * Called when election timeout fires
   * Returns true if election was started, false if already in progress
   */
  startElection() {
    if (this.electionInProgress) {
      this.logger.warn(
        'Election already in progress, ignoring new election request'
      );
      return false;
    }

    // Transition to candidate
    const newTerm = this.state.toCandidate();

    this.electionInProgress = true;
    this.currentElectionTerm = newTerm;

    this.votesReceived.clear();

    // Candidate votes for itself
    this.votesReceived.add(this.state.replicaId);

    this.logger.info(
      `[ELECTION STARTED] term=${newTerm}, replicaId=${this.state.replicaId}`
    );

    return true;
  }

  /**
   * Record a vote received for this candidate
   * Returns true if vote was counted, false if it's for an old term
   */
  recordVote(voterId, term) {
    // Ignore votes from old terms
    if (term !== this.currentElectionTerm) {
      this.logger.warn(
        `Vote from ${voterId} is for old term=${term}, current=${this.currentElectionTerm}`
      );
      return false;
    }

    // Ignore duplicate votes
    if (this.votesReceived.has(voterId)) {
      this.logger.warn(`Duplicate vote from ${voterId}`);
      return false;
    }

    this.votesReceived.add(voterId);

    this.logger.info(
      `[VOTE RECEIVED] from=${voterId}, votes=${this.votesReceived.size}, needed=${this.quorum}`
    );

    return true;
  }

  /**
   * Check if this candidate has won the election.
   * A candidate needs a majority of the cluster votes,
   * including its own vote.
   */
  hasWonElection() {
    return this.votesReceived.size >= this.quorum;
  }

  /**
   * Get current vote count
   */
  getVoteCount() {
    return this.votesReceived.size;
  }

  /**
   * End election (either won or lost)
   * Called when:
   * - Candidate becomes leader (won)
   * - Candidate becomes follower (lost or timeout)
   */
  endElection() {
    if (!this.electionInProgress) {
      return;
    }

    this.logger.info(
      `[ELECTION ENDED] term=${this.currentElectionTerm}, votes=${this.votesReceived.size}`
    );

    this.electionInProgress = false;
    this.votesReceived.clear();
    this.currentElectionTerm = null;
  }

  /**
   * Check if election is in progress
   */
  isElectionInProgress() {
    return this.electionInProgress;
  }

  /**
   * Build RequestVote RPC payload
   * Leader candidates should send this to all peers
   */
  buildRequestVotePayload() {
    const { lastLogIndex, lastLogTerm } =
      this.state.getLastLogIndexAndTerm();

    return {
      term: this.state.currentTerm,
      candidateId: this.state.replicaId,
      lastLogIndex,
      lastLogTerm
    };
  }

  /**
   * Get list of peers to request votes from
   * (excluding self)
   */
  getPeersToVoteRequest() {
    return this.peers || [];
  }

  /**
   * Get current election status (for diagnostics)
   */
  getStatus() {
    return {
      electionInProgress: this.electionInProgress,
      currentTerm: this.currentElectionTerm,
      votesReceived: Array.from(this.votesReceived),
      voteCount: this.votesReceived.size,
      quorumNeeded: this.quorum,
      hasWon: this.hasWonElection()
    };
  }
}

module.exports = ElectionManager;
