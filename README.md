# Multithreaded Equipment Monitoring System

A multithreaded C++ equipment monitoring system that simulates communication between industrial devices and a centralized control server using TCP/IP sockets.

The application demonstrates concurrent network programming, thread synchronization, producer-consumer architecture, centralized equipment monitoring, structured logging, diagnostics, and bidirectional device control.

## Features

- Multithreaded TCP server supporting multiple industrial devices concurrently
- TCP/IP socket communication between independent processes
- Simulated temperature, pressure, RPM, and device status telemetry
- Producer-consumer architecture using a thread-safe queue
- Synchronization using `std::mutex` and `std::condition_variable`
- Centralized sensor monitoring and alert detection
- Thread-safe structured logging
- Network and application error diagnostics
- Bidirectional TCP communication
- Server-to-device control commands
- Automatic `SLOW_DOWN`, `RESUME_NORMAL`, and `SHUTDOWN` actions

## Architecture

```text
                 CENTRAL MONITORING SERVER
                          |
        +-----------------+-----------------+
        |                 |                 |
        v                 v                 v
 Network Thread     Network Thread     Network Thread
        ^                 ^                 ^
        |                 |                 |
   MACHINE_01        MACHINE_02        MACHINE_03
        |                 |                 |
        +-------- Sensor Telemetry ----------+
                          |
                          v
                 Thread-Safe Queue
                          |
                          v
                     Worker Thread
                          |
             +------------+------------+
             |            |            |
             v            v            v
          Parsing      Monitoring     Logging
                          |
                          v
                    Control Logic
                          |
             +------------+-------------+
             |            |             |
             v            v             v
       RESUME_NORMAL  SLOW_DOWN     SHUTDOWN
             |            |             |
             +------ TCP Commands -------+
                          |
                          v
                       Devices
```

## Technologies

- C++17
- POSIX TCP/IP sockets
- `std::thread`
- `std::mutex`
- `std::condition_variable`
- `std::queue`
- CMake
- Multithreading
- Producer-consumer pattern

## Sensor Data

Each simulated device generates:

- Temperature
- Pressure
- RPM
- Equipment status

Example message transmitted over TCP:

```text
MACHINE_01,72.45,31.82,2450,RUNNING
```

TCP messages are newline-delimited, and the server handles partial or combined TCP reads before processing complete equipment messages.

## Monitoring Rules

The centralized server evaluates incoming telemetry and produces alerts for abnormal conditions.

Examples:

```text
[ALERT] MACHINE_01 -> HIGH TEMPERATURE: 95.21 C
[ALERT] MACHINE_02 -> HIGH PRESSURE: 47.82 PSI
[ALERT] MACHINE_03 -> HIGH RPM: 3820
```

## Device Control

The server can send control commands back to individual devices.

### SLOW_DOWN

The device changes its telemetry interval from 2 seconds to 5 seconds.

```text
[CONTROL] MACHINE_01 received command: SLOW_DOWN
[DEVICE] Transmission slowed to every 5 seconds.
```

### RESUME_NORMAL

The normal 2-second telemetry interval is restored.

```text
[CONTROL] MACHINE_01 received command: RESUME_NORMAL
[DEVICE] Normal transmission restored to every 2 seconds.
```

### SHUTDOWN

Critical conditions can trigger an emergency shutdown command.

```text
[CONTROL] MACHINE_01 received command: SHUTDOWN
[EMERGENCY] SHUTDOWN command received!
```

## Producer-Consumer Design

Each connected device is handled by its own network thread.

Network threads act as producers and place received equipment events into a shared thread-safe queue.

A worker thread acts as the consumer and performs:

1. Message parsing
2. Sensor analysis
3. Alert detection
4. Logging
5. Control-command decisions

The queue is protected using `std::mutex`, while `std::condition_variable` allows the worker to sleep efficiently until new data becomes available.

## Logging

Runtime information is stored in:

```text
logs/equipment.log
```

Example:

```text
2026-08-31 14:02:34 [INFO] MACHINE_01 TEMP=42.08 PRESSURE=48.94 RPM=3503 STATUS=WARNING
2026-08-31 14:02:34 [WARNING] MACHINE_01 HIGH_PRESSURE=48.94
2026-08-31 14:02:34 [WARNING] MACHINE_01 HIGH_RPM=3503
2026-08-31 14:02:36 [CONTROL] MACHINE_01 COMMAND=SLOW_DOWN
```

Log writes are protected by a mutex to prevent concurrent threads from corrupting log output.

## Build

### Requirements

- C++17 compatible compiler
- CMake 3.16+
- macOS or Linux

Clone the repository:

```bash
git clone <repository-url>
cd equipment-monitoring-system
```

Create the build directory:

```bash
mkdir build
cd build
```

Configure:

```bash
cmake ..
```

Build:

```bash
cmake --build .
```

Return to the project root:

```bash
cd ..
```

## Run

Start the monitoring server:

```bash
./build/server
```

Open another terminal and start the first device:

```bash
./build/device MACHINE_01
```

Start additional simulated devices:

```bash
./build/device MACHINE_02
```

```bash
./build/device MACHINE_03
```

The monitoring server will process telemetry from all devices concurrently.

## Live Logs

To monitor logs in real time:

```bash
tail -f logs/equipment.log
```

Press `Ctrl + C` to stop following the log.

## Concepts Demonstrated

This project demonstrates:

- TCP client-server architecture
- Network programming
- Multithreaded server design
- Concurrent client handling
- Race-condition prevention
- Mutex-based synchronization
- Condition variables
- Producer-consumer architecture
- TCP message framing
- Error handling
- Structured logging
- Bidirectional communication
- Equipment monitoring and control

## Project Structure

```text
equipment-monitoring-system/
├── src/
│   ├── server.cpp
│   └── device.cpp
├── logs/
│   └── .gitkeep
├── CMakeLists.txt
├── .gitignore
└── README.md
```