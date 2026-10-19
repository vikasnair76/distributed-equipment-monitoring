FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make python3 ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY . .
RUN cmake -S . -B /build -DCMAKE_BUILD_TYPE=Release && cmake --build /build -j2 && ctest --test-dir /build --output-on-failure
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl libstdc++6 && rm -rf /var/lib/apt/lists/* && useradd -m monitor && mkdir -p /data && chown monitor /data
COPY --from=build /build/server /build/device /build/load_generator /usr/local/bin/
COPY configs /configs
USER monitor
WORKDIR /data
CMD ["server", "/configs/monitor-a.conf"]
