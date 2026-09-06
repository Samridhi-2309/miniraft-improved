/**
 * RAFT Protocol Constants and Configuration
 * 
 * These values follow the RAFT specification and are tuned for this project.
 */

module.exports = {
  // Election timeout range (milliseconds)
  // If follower doesn't receive heartbeat within this time, it starts election.
  // Must be >> heartbeat interval (150ms) to reduce split-vote probability.
  // RAFT paper recommends 10-20x heartbeat. Using 1500-3000ms here.
  ELECTION_TIMEOUT_MIN: 1500,
  ELECTION_TIMEOUT_MAX: 3000,

  // Heartbeat interval (milliseconds)
  // Leader sends heartbeats to all followers at this interval
  HEARTBEAT_INTERVAL: 150,

  // RPC request timeout (milliseconds)
  // Wait this long for RPC response before considering it failed
  RPC_TIMEOUT: 5000,

  /**
   * Majority for a cluster of N nodes. Derived, never hardcoded —
   * quorum must always be recomputed from actual cluster size.
   */
  quorumSize: (totalReplicas) => Math.floor(totalReplicas / 2) + 1,

  /**
   * Get random election timeout
   * Returns value between MIN and MAX
   */
  getRandomElectionTimeout: () => {
    return module.exports.ELECTION_TIMEOUT_MIN + 
           Math.random() * (module.exports.ELECTION_TIMEOUT_MAX - module.exports.ELECTION_TIMEOUT_MIN);
  },

  /**
   * Check if a term is valid
   * Used for validation
   */
  isValidTerm: (term) => {
    return typeof term === 'number' && term >= 0;
  },

  /**
   * Check if a replica ID is valid
   */
  isValidReplicaId: (id) => {
    return typeof id === 'string' && id.length > 0;
  }
};
