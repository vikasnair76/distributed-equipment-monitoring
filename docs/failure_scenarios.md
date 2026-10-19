# Failure behavior and diagnosis

| Failure | Expected behavior | Evidence |
|---|---|---|
| Ingress B stops | Device retries same event on C/A, reports FAILOVER; A deduplicates | Integration simulator plus Compose smoke test |
| Leader A stops | B/C refuse writes; no election; devices hold/retry | Integration SIGKILL/refusal/restart checks |
| Live A isolated from C | C refuses TELE; A continues; C catches up when link returns | Delayed/drop proxy test checks actual state |
| Replica offline during mutations | Recover old checkpoint, then install A snapshot | Replica kill/rejoin assertions |
| ACK lost after commit | Retry returns current sequence/command, no stale overwrite | Duplicate and lower-sequence tests |
| TCP splits/coalesces frames | Exactly one response per newline-delimited request | Fragmented and pipelined socket test |
| Client closes with queued work | Mutation can finish; token completion is discarded | Repeated disconnect/new connection checks and sanitizers |
| Full queue | Reads pause on a complete pending frame; bounded queued jobs | Pool capacity test and delayed one-worker overload test |
| Torn trailing append | Truncate incomplete WAL tail and replay prior records | Unit and process restart tests |
| Complete checksum mismatch | Fail startup and preserve files; operator recovery | Byte corruption test expects fatal checksum error |
| Active-load SIGTERM | Stop accepting, join/drain workers, close sockets; retry lost replies | Active socket shutdown and durable state replay |
| Replayed command | Persisted high-water mark suppresses old command application | Monotonic sequence tests, retry logic, persisted shutdown-latch test |

Heartbeats run in a joined background thread. Success records last-seen time; elapsed timeout marks a previously reachable peer suspected. Rejoin is explicit in logs. They cannot prove a crash. Worker saturation, DNS delays, network congestion, a long fsync/checkpoint, or scheduler stalls can yield false positives. Probe execution is sequential, so actual detection delay includes peer RPC time and snapshot transfer, in addition to configured heartbeat/suspect intervals. Tune timeouts above normal tail latency. Suspicion never authorizes leadership changes.

To investigate: compare `monitor_connected_nodes` and `monitor_membership_timeouts_total`, then inspect `event=peer_suspected` / `peer_recovered` with timestamp/node/peer. Rising queue depth and processing histogram tails indicate saturation; low queue depth with client reconnect logs can indicate transport/leader reachability. `event=replication_retry` identifies snapshot problems; persistence failure increments `monitor_wal_write_errors_total` and requires investigation/restart. Check A independently: `/health` on a replica does not prove A is writable.

Metrics are per process and reset on restart. `monitor_failover_events_total` counts device-reported reconnect notices received at that node, not leader elections or an exact durable business event count. `monitor_forwarded_requests_total` counts successful forwarded telemetry RPCs. `monitor_total_events_processed` counts newly accepted leader mutations; `received` includes duplicate/invalid TELE attempts and requests that must be forwarded. Invalid requests not shaped as TELE can also count as invalid. Rejected counts track connection-cap refusals; client failures from other causes are reported by the load generator. The WAL error counter currently includes Store apply errors such as identity capacity, not exclusively failed write syscalls. Processing histogram covers worker request latency including queue wait, all request types, and forwarding; it excludes outbound socket drain. Client ACK latency is measured separately by the load tool. CPU is cumulative process seconds; memory is peak resident size, not instantaneous heap size.

Automated faults use deterministic application-processing delay (`delay_ms`), a delayed/drop TCP proxy, SIGKILL/SIGTERM, and byte edits of disposable WALs. They do not simulate a host power cut, disk firmware lying about fsync, arbitrary Byzantine peers, or every filesystem crash point. Sanitizer success is evidence for tested schedules, not a proof of race freedom. Filesystem ENOSPC/EIO and every resource-exhaustion path remain future fault-injection coverage.
