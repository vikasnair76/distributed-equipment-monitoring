# Decisions and implementation status

| Phase | Implemented and exercised | Deliberate scope |
|---|---|---|
| 1: stabilize | RAII descriptors, bounded closeable queue, joined workers, signal shutdown, unit/integration tests | Replaced unsafe transport code; legacy source retained for review |
| 2: network/concurrency | Linux epoll, macOS poll, bounded worker admission, connection generations, measured 100/500/1000 clients | One storage mutex; no zero-copy or strict queue fairness |
| 3: multi-node | Independent A/B/C, IDs, peer probes, suspect/rejoin, device reconnect with jitter and persisted identity | Membership is configured; no discovery or distributed ownership leases |
| 4: persistence | Versioned CRC WAL, fsync ACK ordering, replay, torn-tail repair, atomic checkpoints, sequence dedup | Latest-state compaction; no full event archive/offline device spool |
| 5: replication | Fixed-writer forwarding, durable replica snapshots, partition refusal, catch-up and regression checks | Asynchronous latest-state replication; no Raft, elections, quorum ACK, or automatic leader failover |
| 6: operate/measure | Prometheus endpoint/scraping, Compose health checks/volumes, C++ load utility, benchmark and restart measurements | Short laptop baselines; no six-nines claim, certified control, or long-duration capacity qualification |

These six scoped phases have working baseline implementations. This does not make every aspirational reliability property in the prompt complete. Automatic leader election was explicitly excluded, as permitted by the design constraint. Longer soak tests, storage fault injection, randomized concurrency testing, staged rollout tooling, TLS/auth, incremental catch-up, independent metrics serving under saturation, and statistically repeated benchmark trials remain work.

Why fixed leader: correctness can be explained without pretending heartbeat-based promotion is consensus. Endpoint failover keeps ingress flexible; it cannot make the only authority highly available. Why full snapshots: small bounded state is easier to validate atomically than a log-retention/index protocol; catch-up bandwidth and checkpoint cost are the ceiling. Why one mutex: it provides an explicit persist-before-state ordering and simple dedup; group commit is justified only after measurement. Why newline text: preserve the existing framing concept with limits and strict validation, without a serializer dependency. Why one telemetry in flight per device: bounded retry ownership and deterministic command ordering at the cost of per-device throughput.

One shared port keeps deployment simple. Health and metrics share workers with telemetry, so overload can delay them and trigger health-check failures. Container health status does not itself restart an unhealthy container; Compose restart policies restart exited processes. Peers and simulator retry instead of assuming startup order means readiness.

The configuration compares endpoints as strings and trusts one consistent leader assignment. It is not safe to dynamically promote replicas or deploy two independent configurations claiming authority. Deployment config is part of the declared model. Local data directory locking prevents two processes using one directory; it is not cluster fencing.

Interview-ready resume bullets grounded in implemented work:

- Built a C++20 equipment monitor with Linux epoll, bounded worker queues, per-connection generation tokens, and automated overload/disconnect/shutdown checks.
- Implemented checksummed WAL replay, fsync-before-ACK persistence, atomic checkpoints, sequence deduplication, and fixed-leader snapshot replication with partition refusal.
- Added multi-endpoint device recovery, monotonic command handling, Prometheus metrics, a five-service Compose deployment, sanitizer tests, and reproducible 100–1,000-client load and crash-recovery measurements.
