# Executed validation

Date: 2026-10-19. Upstream baseline: `d1b743efd541723ca50f738feff286c748c615b0`. Work branch: `codex/distributed-monitoring`. These results describe actual local execution, not hosted CI status.

| Build/environment | Command | Result |
|---|---|---|
| macOS ARM64, AppleClang 21, Release | `cmake -S . -B build -DCMAKE_BUILD_TYPE=Release`; build; `ctest --test-dir build --output-on-failure` | 2/2 passed, 4.93s |
| macOS ARM64, Debug, AddressSanitizer + UndefinedBehaviorSanitizer | configure with `-DSANITIZER=address,undefined`; build; CTest | 2/2 passed, 6.30s |
| macOS ARM64, Debug, ThreadSanitizer | configure with `-DSANITIZER=thread`; build; CTest | 2/2 passed, 9.01s |
| Debian bookworm ARM64, GCC 12.2, Release, Linux epoll, Docker build stage | `docker build -t equipment-monitor:local .` | 2/2 passed, 4.15s; no compiler warnings in final build |
| Compose, all five services | `docker compose -p equipment-validation up -d --build`; `tests/compose_smoke.py` with alternate port environment | Node health, device ingress failover/progress, replica rejoin, three Prometheus targets verified; raw JSON in `deployment_validation.json` |
| Benchmark, macOS Release | `python3 benchmarks/run.py --seconds 5` | 100/500/1000 clients measured; `benchmark_results.json` |
| Crash/restart benchmark | `python3 benchmarks/recovery.py build` | 100 reconnect samples; durable pre-crash probe preserved; `recovery_results.json` |

Each CTest registration runs a collection of checks rather than one narrow assertion:

- Unit: CRC known vector; strict numeric/status/sequence parsing; preserved warning and critical rules; durable append/dedup; checkpoint replay; incomplete tail repair; complete corruption rejection; directory lock exclusion; consistent snapshots; rejection of regressions and equal-sequence conflicts; rotation across restart; bounded pool admission and drain.
- Integration: real server processes and sockets; fragmented/coalesced TCP; lower/duplicate sequences; invalid readings; forwarded leader writes; a delayed/drop TCP proxy partition with A still live; replica SIGKILL/rejoin; disconnect/descriptor reuse; leader SIGKILL/write refusal/replay; command ACK validation; actual simulator ingress failure and retained identity; saved SHUTDOWN latch; deterministic queue overload; metrics; torn WAL tail; byte-corrupted complete record; active-connection shutdown/recovery.

The Dockerfile runs CTest before producing the runtime image. Deployment uses separate volumes and runtime non-root user. Existing host port 8080 was already occupied, so validation used 18080/18081/18082/19090 without changing the unrelated service. Test deployment resources are removed after verification; default `docker compose up -d --build` recreates a fresh deployment.

The host sandbox blocks `ps` and hardware sysctl details. The benchmark therefore samples the server's getrusage-based CPU and peak RSS metrics rather than inventing host resource figures. Hardware model/physical RAM remain unavailable. Linux performance and host-power-loss durability were not measured. Sanitizers passed the exercised executions; they do not prove all schedules free of races or memory errors. No automatic leader election or consensus invariant is claimed.
