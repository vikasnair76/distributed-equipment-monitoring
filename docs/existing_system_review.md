# Existing system review

Reviewed upstream commit `d1b743efd541723ca50f738feff286c748c615b0` before changes, including both source files, CMake, and README. The original source and README remain in `legacy/` for comparison and are excluded from the build.

| Location | Finding | Consequence and replacement |
|---|---|---|
| `server.cpp::main`, `handleClient` | Detached consumer and thread per accepted socket; infinite loops | No coordinated join or drain; threads exhaust resources with idle clients. Reactor owns sockets; fixed pool drains and joins. |
| `ThreadSafeQueue` | No capacity or close predicate | Unbounded memory; consumer cannot exit. Bounded nonblocking admission, condition-variable workers, close/drain semantics. |
| `DeviceEvent::clientSocket` | Queued work retains a bare descriptor after connection closes | Descriptor reuse can deliver a command to another device. Monotonic connection tokens; only reactor writes/closes sockets. |
| `lastCommandBySocket`, `sendControlCommand` | Dedup state keyed by descriptor; marked sent before send succeeds | A failed command is suppressed; socket reuse loses identity. Commands keyed by stable device and telemetry sequence, retried with telemetry until CACK. |
| `handleClient`, device `receiveCommands` | Newline framing supports partial/coalesced reads, but buffers have no maximum | Memory exhaustion from an unterminated frame. Retain newline framing with strict frame bounds. |
| Both `sendAll` implementations | Blocking writes with no deadline and no EINTR retry | Slow receivers block execution indefinitely; signals look like disconnects. Nonblocking buffers in reactor; bounded-deadline client RPC. |
| `parseMessage` | `stod`/`stoi` accept prefixes; no finite/range/status checks | Invalid numeric values enter control decisions. Strict token parsing, finite/range checks, bounded identity alphabet. |
| Server logging | Opens file per message; creates no logs directory; frequent synchronous console writes | Logging can dominate the sole consumer and fails when directory is missing. Structured stderr outside reactor; container/runtime owns collection. |
| Monitoring rules | Sensible warning/critical thresholds and three commands | Preserve thresholds exactly; evaluate equipment status too. |
| Device networking | One fixed address, process stops on disconnect | No reconnect, retained identity/sequence, retries, or failover. Multi-endpoint retry loop with jitter and persisted command high-water mark. |
| Shutdown paths | `close(serverSocket)` is unreachable; receiver join requires explicit shutdown | No signal-driven orderly stop. Signal handler sets only `sig_atomic_t`; reactor stops admission and pool joins. |
| Persistence/distribution | Telemetry text log only; no recovery, membership, replication, or durable ACK | Add separate WAL/checkpoint storage and explicitly limited fixed-leader replication. |

The original code uses standard containers and stack objects rather than raw heap allocation; the principal memory risks are unbounded growth and unmanaged thread/socket lifetimes, not a demonstrated raw-pointer leak. The original mutexes protect their maps/streams but do not make descriptor identity safe.

A replacement reactor was warranted: wrapping the detached client threads would retain the scaling and identity defects. Useful domain rules, simulator feedback behavior, producer/consumer synchronization, and newline framing were retained. No assertion is made that the original project was sanitizer-tested; only the new build has recorded test evidence.
