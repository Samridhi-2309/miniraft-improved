const { RPC_TIMEOUT } = require('../replicas/common/constants');

class LeaderRouter {
  constructor(replicaEndpoints = [], logger) {
    this.replicas = replicaEndpoints.filter(Boolean);
    this.logger = logger;
    this.currentLeader = null; // URL string
  }

  getLeader() { return this.currentLeader; }

  async discoverLeader() {
    this.logger.info('Discovering leader among replicas');

    for (const r of this.replicas) {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), RPC_TIMEOUT);

      try {
        const res = await fetch(`${r}/state`, {
          signal: controller.signal
        });

        if (!res.ok) continue;

        const json = await res.json();

        if (json.role === 'leader') {
          this.currentLeader = r;
          this.logger.info(`Leader discovered: ${r}`);
          return r;
        }
      } catch (err) {
        const message = err.name === 'AbortError' ? 'timeout' : err.message;
        this.logger.warn(`discoverLeader: ${r} -> ${message}`);
      } finally {
        clearTimeout(timeoutId);
      }
    }

    this.logger.warn('No leader discovered');
    this.currentLeader = null;
    return null;
  }

  async sendCommand(command) {
    if (!command) throw new Error('command required');

    // Try current leader first
    const tryPost = async (leaderUrl) => {
      this.logger.event('ROUTE', { action: 'post_attempt', to: leaderUrl });

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), RPC_TIMEOUT);

      try {
        const res = await fetch(`${leaderUrl}/command`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ command }),
          signal: controller.signal
        });

        if (!res.ok) {
          const txt = await res.text().catch(() => '');
          throw new Error(`status=${res.status} ${txt}`);
        }

        this.logger.event('ROUTE', { action: 'post_success', to: leaderUrl });
        return await res.json();
      } catch (err) {
        const message = err.name === 'AbortError' ? 'timeout' : err.message;
        this.logger.warn(`sendCommand -> ${leaderUrl} failed: ${message}`);
        throw new Error(message);
      } finally {
        clearTimeout(timeoutId);
      }
    };

    if (this.currentLeader) {
      try {
        return await tryPost(this.currentLeader);
      } catch (err) {
        // fallthrough to discovery
        this.logger.event('ROUTE', { action: 'failover', reason: err.message });
      }
    }

    // Discover and retry
    const discovered = await this.discoverLeader();
    if (discovered) return await tryPost(discovered);

    throw new Error('No leader available to accept command');
  }
}

module.exports = LeaderRouter;
