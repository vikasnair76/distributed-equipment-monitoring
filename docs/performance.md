# Measured performance

Measurements collected on 2026-10-19. Raw data: [benchmark_results.json](benchmark_results.json), [recovery_results.json](recovery_results.json), and [deployment_validation.json](deployment_validation.json). These are measured local baselines, not production capacity or availability guarantees.

Environment: macOS-26.5.2-arm64-arm-64bit-Mach-O, arm64, 8 logical CPUs; Apple clang version 21.0.0 (clang-2100.1.1.101); CMake Release. Hardware model and physical RAM were unavailable under host sandbox restrictions. Server and load generator share the host; background container activity was present. Linux epoll correctness was tested separately inside Debian ARM64 Docker; these throughput figures use the macOS poll fallback.

Each row: fresh node/data directory, four workers, queue 256, sync=always, loopback, 10 events/s per client, five seconds, one event in flight per device, no padding, no bursts. Snapshot peers disabled to isolate the ingestion baseline. No warmup/repeat confidence intervals. Server CPU is the maximum sampled delta of getrusage CPU time / wall time (100% = one core). RSS is the maximum reported process high-water mark. Samples are approximately every 200ms and can miss peaks.

| Clients | ACK events/s | p50 ms | p95 ms | p99 ms | Failed attempts | Peak sampled CPU % | Peak RSS MiB | Max sampled queue |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 979.08 | 1.69 | 8.33 | 14.84 | 0 | 19.14 | 1.88 | 17 |
| 500 | 4894.13 | 1.45 | 13.68 | 68.98 | 0 | 50.03 | 2.27 | 48 |
| 1000 | 9781.48 | 8.29 | 21.40 | 42.60 | 0 | 69.17 | 2.53 | 253 |

Successful-attempt ACK latency includes connecting when needed, sending, queueing, processing, fsync, and response receipt. It excludes failed attempts/backoff; do not read it as an all-events outage-inclusive latency distribution. Closed-loop clients reduce offered load while waiting. Throughput below the nominal rate includes launch/scheduling effects; no saturation capacity is inferred. A successful ACK count is not a count of distinct durable new events in a workload with retries. Failures combine network errors and rejected/RETRY responses; they are not a direct measure of lost acknowledged writes.

The restart run used 100 clients at 20 Hz for six seconds, SIGKILL after two seconds, then an intentional 300ms outage. Its 100 reconnect samples had p95 **329.34ms**; restart-to-PING readiness was **20.53ms** with 10ms polling. A pre-crash durable probe survived replay. There were **598 failed attempts** and **10,724 successful ACK attempts**. The intentional delay dominates the reconnect measurement; this is not an uncontrolled outage SLA.

The Compose stop/rejoin smoke test observed device progress after stopping its ingress replica and restored three healthy Prometheus scrape targets. Its approximately 2.34s observation includes docker stop time, telemetry cadence, and polling; it is not comparable to the restart benchmark.

Reproduce:

```bash
python3 benchmarks/run.py --build build --seconds 5
python3 benchmarks/recovery.py build
./build/load_generator 127.0.0.1:8080 1000 60 10 512 10
```

The C++ utility accepts endpoints, clients, seconds, per-client Hz, padding bytes, and burst size. Frequency is a target, bounded by ACK latency. For longer/saturation trials vary workers, queue, fsync policy, number of replicas, duration, client hardware, and burst size. Compare offered versus achieved rate, failed attempts, queue occupancy, CPU, memory, and reconnect samples. The client latency sample vectors grow with the finite trial length; use shorter windows or a streaming histogram for very long trials. Server memory bounds do not imply bounded benchmarking-client sample storage.
