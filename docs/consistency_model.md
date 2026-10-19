# Consistency, authority, and durability

## Authority and ordering

A is the fixed authoritative writer. A process is authoritative only when its configured `listen` endpoint equals its configured `leader` endpoint. All replicas must name the same A; deployment configuration is trusted. This is not a mechanism that can detect arbitrary operator misconfiguration, hostile peers, or two independently configured leaders. Do not change a replica's leader setting to itself during an outage.

Ingress on B/C forwards each TELE request to A. A serializes validation of device sequence, append, sync, and state mutation with one storage mutex. That critical section defines the total accepted mutation order. State exposes the highest accepted sequence per device, not an ordered event query API. Missing sequence numbers are allowed. Equal/lower sequences never overwrite state, even if their payload differs. Retrying stale telemetry receives the current state and current command sequence.

Each device ID must have one sequence-generating owner. The simulator locks its state file and preserves sequence allocation over restart. This is local ownership, not distributed leases: two machines deliberately using the same identity are not supported. A socket is a transport session, not an owner epoch.

A successful `ACK device seq command` means A has accepted that sequence (or already has a newer one) under its configured persistence policy. A replica's forwarding ACK has the same leader-local guarantee. There is no quorum commitment or guaranteed replicated ACK. A failure after persistence but before delivery leaves the client uncertain; retrying is safe.

## Replication

B/C periodically request `SNAPSHOT` from A. A returns a consistent point-in-time set of latest device records while holding the storage mutex. B/C validate the entire snapshot, reject missing/regressing devices and equal-sequence conflicting payloads, and install it with an atomic local checkpoint. This is state replication, not a complete history replica. Offline replicas catch up with a full snapshot after rejoin. They never apply forwarded writes speculatively.

Replica `STATE` reads can lag; leader `STATE` reads are serialized with writes. A forwarded ACK can arrive before the ingress replica's local state catches up. No cross-node read-after-write guarantee is offered for local `STATE` reads. Snapshots are capped at 4 MiB and state at 10,000 identities, with no automatic eviction. Full snapshot transfer and the storage mutex impose a practical scaling ceiling; incremental replication is a future improvement.

If A becomes unreachable from B/C, those nodes return `RETRY leader_unavailable` for telemetry and command ACK forwarding. If A remains reachable elsewhere in a partition, it can continue accepting writes. Reconnecting devices may find A directly. B/C do not elect or promote a writer. Restoring A with its intact disk resumes operation. Restoring an older backup can cause replicas to reject its regressed state; an operator must reconcile rather than silently roll back.

Permanent loss of A's disk can lose acknowledged writes not yet present on replicas. Recovery from B/C is a manual data-reconciliation operation, not an automatic failover procedure. Future automatic leadership requires persistent terms, votes, quorum commitment, and a correct consensus implementation.

## WAL and checkpoints

Version 1 record: three network-byte-order uint32 fields (`MON1` magic/version `0x4d4f4e31`, payload length, IEEE CRC32 of payload), followed by canonical TELE text. Payload is limited to 4,096 bytes; checkpoint/response limits bound state memory. Padding is not persisted. WAL has exclusive directory ownership via `flock`.

In `sync=always` (default), append all record bytes, `fsync` WAL, update state, then generate ACK. If append/sync/checkpoint fails, poison that Store: subsequent writes, reads, and snapshots fail until restart. This prevents continued writes behind a potentially incomplete record. Some failed requests may have reached disk; clients must retry, never assume failure means absence.

Every `rotate` mutations (default 10,000), write the latest state to a temporary checkpoint, fsync it, rename over `checkpoint`, fsync its directory, truncate and fsync the WAL. A crash between rename and truncation leaves redundant WAL records; sequence dedup makes replay safe. A replica follows the same checkpoint ordering. Checkpoints compact history; they are not archival telemetry storage.

Startup replays checkpoint then WAL, validating every complete record. An incomplete WAL tail is truncated to the last complete record and synced. An invalid header, checksum mismatch, or incomplete checkpoint fails startup; preserve the files for investigation/backup recovery. The system does not discard arbitrary corrupted complete records and call recovery successful.

`sync=os` ACKs after successful writes to the OS page cache, with no fsync promise. It can lose acknowledged events after host/power failure. Default fsync mode survives process crashes and relies on the filesystem/device's fsync contract for host failure. macOS `fsync` is not `F_FULLFSYNC`; neither mode protects against disk loss, faulty storage firmware, or a broken filesystem. Newly created parent directories should be provisioned on persistent storage before a deployment that needs power-loss guarantees. Actual host power-loss testing has not been performed.

## Commands

Command ID is the pair `(device_id, accepted_sequence)`; there is no independent leader epoch because leadership never changes automatically. ACK carries both telemetry acknowledgement and a command. The device rejects older ACK sequences, saves the accepted command sequence/action before applying the simulated effect, and sends `CACK device seq`. Lost CACK responses cause the same telemetry/command to be retried and deduplicated. Newer server state invalidates stale CACKs.

The device saves sequence allocation before transmission, so restart can leave gaps and can abandon an unacknowledged in-memory reading. This is not an offline event spool. Durable server acknowledgements remain recoverable on A; unacknowledged device observations have no delivery promise across device process failure.

SLOW_DOWN changes cadence from 2 to 5 seconds by default; RESUME_NORMAL restores it. SHUTDOWN latches in the device state file and survives process restart. While disconnected the stop-and-wait simulator stays in hold and sends no fresh observations; loss of contact never generates RESUME_NORMAL. A real actuator cannot get exactly-once physical effects merely by writing a local file. This is simulated control and is not safety-certified industrial software.
