#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>
namespace monitor {
using Clock = std::chrono::steady_clock;
constexpr size_t frame_limit = 4096, snapshot_limit = 4 * 1024 * 1024, device_limit = 10000;
struct Fd {
    int value = -1;
    explicit Fd(int n = -1) : value(n) {}
    ~Fd() {
        if (value >= 0)
            ::close(value);
    }
    Fd(const Fd &) = delete;
    Fd &operator=(const Fd &) = delete;
    Fd(Fd &&o) noexcept : value(std::exchange(o.value, -1)) {}
    Fd &operator=(Fd &&o) noexcept {
        if (this != &o) {
            if (value >= 0)
                ::close(value);
            value = std::exchange(o.value, -1);
        }
        return *this;
    }
};
struct Event {
    std::string device, status;
    uint64_t seq = 0;
    double temperature = 0, pressure = 0;
    int rpm = 0;
    std::string encode() const;
    static Event parse(const std::string &);
    std::string command() const;
};
class Pool {
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::function<void()>> jobs;
    size_t capacity;
    bool closed = false;
    std::vector<std::thread> threads;

  public:
    Pool(size_t workers, size_t capacity);
    ~Pool();
    bool submit(std::function<void()> job);
    size_t depth();
    void stop();
};
uint32_t checksum(const std::string &);
class Store {
    std::filesystem::path directory;
    Fd lock_file, wal;
    bool sync;
    size_t writes = 0, rotate_every;
    bool poisoned = false;
    std::map<std::string, Event> state;
    mutable std::mutex mu;
    void append(const Event &);
    void checkpoint();
    void replay();

  public:
    Store(std::filesystem::path path, bool sync = true, size_t rotate = 10000);
    std::pair<Event, bool> apply(const Event &);
    std::string snapshot() const;
    void install(const std::string &);
    std::optional<Event> get(const std::string &) const;
};
struct Config {
    std::string id = "a", listen = "127.0.0.1:8080", leader = "127.0.0.1:8080", data = "data/a";
    std::vector<std::string> peers;
    size_t workers = 4, queue = 256, max_connections = 4096, rotate = 10000;
    int timeout_ms = 30000, heartbeat_ms = 500, suspect_ms = 2000, delay_ms = 0;
    bool sync = true;
    static Config load(const std::string &);
};
std::vector<std::string> split(const std::string &, char);
Fd connect_to(const std::string &, int timeout_ms = 1000);
std::string rpc(const std::string &, const std::string &, size_t limit = frame_limit,
                int timeout_ms = 1000);
bool send_all(int, const std::string &, int timeout_ms = 1000);
std::string read_line(int, size_t limit = frame_limit, int timeout_ms = 1000);
void log(const std::string &node, const std::string &level, const std::string &type,
         const std::string &context = "");
void serve(const Config &);
} // namespace monitor
