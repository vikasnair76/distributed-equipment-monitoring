# Fault-Tolerant Distributed Equipment Monitoring Platform

A working C++20 equipment monitor with nonblocking TCP, Linux epoll, bounded workers, persistent device state, crash recovery, replica catch-up, and observable failure handling. Evolved from the original C++17 project after an [actual source review](docs/existing_system_review.md); original code remains in `legacy/`.

**The consistency model is deliberately narrow:** A is the fixed writer; B/C forward writes and replicate snapshots asynchronously. Devices recover from an ingress outage. A leader outage pauses writes until A returns. There is no automatic leader election, quorum durability, or claim of production-grade availability. This is a simulated controller, not safety-certified equipment software.

## Before and after

```text
Before: device → blocking thread per client → unbounded queue → one consumer → text logs

After:                         fixed authoritative writer
  devices ───→ A ingress ─────→ bounded queue/workers ───→ WAL + checkpoint
      ├──────→ B ingress ──forward──┘                         │
      └──────→ C ingress ──forward──┘                         │
                  B local checkpoint ←── snapshot pull ──────┤
                  C local checkpoint ←── snapshot pull ──────┘
                      A / B / C ── /metrics ──→ Prometheus
```

See the [node diagram and concurrency design](docs/architecture.md), [consistency model](docs/consistency_model.md), [failure scenarios](docs/failure_scenarios.md), and [decisions/status](docs/design_decisions.md).

## Build and test

Requires a C++20 compiler, CMake 3.20+, POSIX sockets, and Python 3 for test harnesses. Runtime C++ code has no external library dependencies. Linux uses epoll; macOS uses poll.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

Sanitizers (separate builds; do not combine TSan with ASan):

```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DSANITIZER=address,undefined
cmake --build build-asan -j4
ctest --test-dir build-asan --output-on-failure
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DSANITIZER=thread
cmake --build build-tsan -j4
ctest --test-dir build-tsan --output-on-failure
```

Executed validation and exact limits are recorded in [validation.md](docs/validation.md). CI defines Linux/macOS sanitizer jobs; local execution does not imply those hosted jobs have run.

## Run locally

Run each server in a separate terminal from the repository root:

```bash
./build/server configs/local-a.conf
./build/server configs/local-b.conf
./build/server configs/local-c.conf
```

Then:

```bash
./build/device MACHINE_01 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8080 data/MACHINE_01.state
curl -fsS http://127.0.0.1:8080/metrics
./build/load_generator 127.0.0.1:8080 100 10 10
```

The simulator takes ID, comma-separated endpoints, state-file path, optional interval in milliseconds (default 2000), and optional event count (0 = continuous). Keep the state file with its device identity across restarts. One active simulator per identity/state file. `Ctrl-C` stops a node gracefully. Stop B to see reconnection; stop A to see explicit RETRY/hold until recovery. State directories are independent. The server listens on all interfaces at the configured port; local deployments must use a trusted network/firewall.

## Docker Compose

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f device-simulator monitor-a monitor-b monitor-c
curl -fsS http://127.0.0.1:8080/metrics
```

Services: `monitor-a`, `monitor-b`, `monitor-c`, `device-simulator`, `prometheus`. Each node/device has its own persistent volume. Prometheus is available at `http://127.0.0.1:9090`. Health checks test responsiveness; retry loops tolerate startup races. CPU/memory limits keep default services modest. `/health` is process health, not cluster write readiness.

If ports are occupied, set `MONITOR_A_PORT`, `MONITOR_B_PORT`, `MONITOR_C_PORT`, and `PROMETHEUS_PORT` before every Compose command. Internal service endpoints remain unchanged. For example:

```bash
export MONITOR_A_PORT=18080 MONITOR_B_PORT=18081 MONITOR_C_PORT=18082 PROMETHEUS_PORT=19090
docker compose up -d --build
```

Failure/recovery demonstration:

```bash
docker compose stop monitor-b
docker compose logs --tail=30 device-simulator monitor-c
docker compose start monitor-b
docker compose stop monitor-a
# B/C stay responsive but cannot commit TELE; device holds and retries.
docker compose start monitor-a
curl -fsS http://127.0.0.1:${MONITOR_A_PORT:-8080}/metrics
docker compose run --rm --no-deps device-simulator load_generator monitor-a:8080,monitor-b:8080,monitor-c:8080 500 30 10
```

`docker compose down` stops the deployment and retains data volumes. `down -v` deletes them; use it only for disposable tests. The manual Compose smoke harness stops/restarts its own ingress replica and verifies device progress and all three Prometheus targets:

```bash
COMPOSE_PROJECT_NAME=equipment-validation python3 tests/compose_smoke.py
```

Start that project first using `docker compose -p equipment-validation up -d --build`. Keep matching port environment variables in both commands.

## Components and guarantees

| Component | Responsibility |
|---|---|
| `src/server.cpp` | Reactor, generation-token sockets, bounded admission, completion buffers, request handling, metrics, peer probes and replication loop |
| `src/core/model.cpp` | Strict event validation, preserved thresholds, bounded closeable worker pool, config, structured logs |
| `src/core/storage.cpp` | Exclusive data-directory ownership, CRC/versioned WAL, replay, sequence dedup, checkpoint ordering, replica snapshot validation |
| `src/core/network.cpp` | RAII sockets and bounded-deadline nonblocking outbound RPC |
| `src/device.cpp` | Persisted sequence/command state, stop-and-wait retries, endpoint rotation, bounded exponential backoff with jitter, command ACK and shutdown latch |
| `benchmarks/` | C++ clients, CPU/RSS/queue sampling, crash/restart measurement |
| `tests/` | Unit invariants, real-process faults and partition proxy, manual Compose validation |

Warning rules preserve temperature >90°C, pressure >45 PSI, or RPM >3500; critical temperature >99.5°C or pressure >49.8 PSI produces SHUTDOWN. WARNING/STOPPED equipment status also participates in control. Commands are `SLOW_DOWN`, `RESUME_NORMAL`, `SHUTDOWN`, identified by `(device, sequence)`. A device saves its command high-water mark before applying a simulated effect; a saved SHUTDOWN remains latched on restart.

A full worker queue pauses reads rather than allocating more work. Per-connection frames are limited to 4 KiB; connection count, queue, device cardinality, snapshots, and retained responses are bounded. Only the reactor operates accepted sockets. Workers drain on shutdown; a client may need to retry a committed write whose response was lost.

Default `sync=always` appends and fsyncs before ACK. Torn trailing WAL records are repaired; complete checksum corruption fails startup. Checkpoints bound historical WAL growth by compacting to latest state. Replicas never accept authoritative writes; snapshots reject regressions. See [the exact crash/host-loss caveats](docs/consistency_model.md) before interpreting durability.

## Measurements

A five-second macOS ARM64 loopback run with 1,000 clients at target 10 Hz achieved **9,781.48 ACK events/s**, **21.40ms p95**, and **zero failed attempts**. This tests macOS poll; the Linux epoll build passed separate real-process correctness tests. CPU, peak RSS, queue samples, 100/500-client results, and methodology are in [performance.md](docs/performance.md), with raw JSON evidence.

```bash
python3 benchmarks/run.py --seconds 5
python3 benchmarks/recovery.py build
./build/load_generator 127.0.0.1:8080 1000 60 10 512 10
```

Load-generator arguments: endpoints, clients, duration seconds, per-client Hz, padding bytes, burst size. The load tool reports ACK throughput, p50/p95/p99, failed/rejected attempts, and reconnect samples. Successful-attempt latency excludes failed attempts/backoff; it is not an outage-inclusive latency guarantee. Benchmark clients use one thread per connection, independently of the event-driven server.

## Limitations and next work

No automatic leader election, quorum commitment, replicated-write durability guarantee, TLS/auth, distributed device lease, full telemetry archive, or offline device spool. Replica reads can lag. Full snapshots and one storage mutex trade scale for clear ordering. Heartbeat suspicions can be false; they never trigger promotion. Host/disk-loss and filesystem error campaigns, long soak tests, repeated benchmark trials, incremental replication, group commit, and independent metrics serving remain future milestones. The six scoped implementation phases and three defensible resume bullets are in [design_decisions.md](docs/design_decisions.md).
