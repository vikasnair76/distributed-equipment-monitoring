#include "core.hpp"
#include <charconv>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
namespace monitor {
std::vector<std::string> split(const std::string &s, char c) {
    std::vector<std::string> r;
    std::istringstream in(s);
    std::string v;
    while (std::getline(in, v, c))
        r.push_back(v);
    return r;
}
std::string Event::encode() const {
    std::ostringstream s;
    s << std::setprecision(17) << "TELE " << device << ' ' << seq << ' ' << temperature << ' '
      << pressure << ' ' << rpm << ' ' << status;
    return s.str();
}
Event Event::parse(const std::string &line) {
    if (line.size() > frame_limit)
        throw std::runtime_error("frame_too_large");
    Event e;
    std::istringstream in(line);
    std::string tag, padding, sequence;
    if (!(in >> tag >> e.device >> sequence >> e.temperature >> e.pressure >> e.rpm >> e.status) ||
        tag != "TELE" || sequence.empty() ||
        sequence.find_first_not_of("0123456789") != std::string::npos ||
        std::from_chars(sequence.data(), sequence.data() + sequence.size(), e.seq).ec !=
            std::errc{} ||
        e.seq == 0 || e.seq == std::numeric_limits<uint64_t>::max() || e.device.empty() ||
        e.device.size() > 64 ||
        e.device.find_first_not_of(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") !=
            std::string::npos ||
        !std::isfinite(e.temperature) || !std::isfinite(e.pressure) || e.temperature < -273.15 ||
        e.temperature > 1000 || e.pressure < 0 || e.pressure > 100000 || e.rpm < 0 ||
        e.rpm > 1000000 ||
        (e.status != "RUNNING" && e.status != "WARNING" && e.status != "STOPPED"))
        throw std::runtime_error("invalid_event");
    if (in >> padding) {
        if (padding.find_first_not_of('x') != std::string::npos)
            throw std::runtime_error("invalid_padding");
        std::string extra;
        if (in >> extra)
            throw std::runtime_error("extra_fields");
    }
    return e;
}
std::string Event::command() const {
    if (temperature > 99.5 || pressure > 49.8 || status == "STOPPED")
        return "SHUTDOWN";
    if (temperature > 90 || pressure > 45 || rpm > 3500 || status == "WARNING")
        return "SLOW_DOWN";
    return "RESUME_NORMAL";
}
Pool::Pool(size_t n, size_t cap) : capacity(cap) {
    if (!n || !cap)
        throw std::runtime_error("invalid_pool_size");
    try {
        for (size_t i = 0; i < n; ++i)
            threads.emplace_back([this] {
                for (;;) {
                    std::function<void()> job;
                    {
                        std::unique_lock lock(mu);
                        cv.wait(lock, [this] { return closed || !jobs.empty(); });
                        if (jobs.empty())
                            return;
                        job = std::move(jobs.front());
                        jobs.pop_front();
                    }
                    job();
                }
            });
    } catch (...) {
        stop();
        throw;
    }
}
Pool::~Pool() {
    stop();
}
void Pool::stop() {
    {
        std::lock_guard lock(mu);
        closed = true;
    }
    cv.notify_all();
    for (auto &t : threads)
        if (t.joinable())
            t.join();
}
bool Pool::submit(std::function<void()> job) {
    std::lock_guard lock(mu);
    if (closed || jobs.size() >= capacity)
        return false;
    jobs.push_back(std::move(job));
    cv.notify_one();
    return true;
}
size_t Pool::depth() {
    std::lock_guard lock(mu);
    return jobs.size();
}
Config Config::load(const std::string &path) {
    Config c;
    std::ifstream f(path);
    if (!f)
        throw std::runtime_error("cannot_open_config:" + path);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#')
            continue;
        auto p = line.find('=');
        if (p == std::string::npos)
            throw std::runtime_error("invalid_config_line");
        auto k = line.substr(0, p), v = line.substr(p + 1);
        if (k == "id")
            c.id = v;
        else if (k == "listen")
            c.listen = v;
        else if (k == "leader")
            c.leader = v;
        else if (k == "data")
            c.data = v;
        else if (k == "peers")
            c.peers = split(v, ',');
        else if (k == "workers")
            c.workers = std::stoul(v);
        else if (k == "queue")
            c.queue = std::stoul(v);
        else if (k == "max_connections")
            c.max_connections = std::stoul(v);
        else if (k == "rotate")
            c.rotate = std::stoul(v);
        else if (k == "timeout_ms")
            c.timeout_ms = std::stoi(v);
        else if (k == "heartbeat_ms")
            c.heartbeat_ms = std::stoi(v);
        else if (k == "suspect_ms")
            c.suspect_ms = std::stoi(v);
        else if (k == "delay_ms")
            c.delay_ms = std::stoi(v);
        else if (k == "sync") {
            if (v != "always" && v != "os")
                throw std::runtime_error("invalid_sync");
            c.sync = v == "always";
        } else
            throw std::runtime_error("unknown_config:" + k);
    }
    if (c.id.empty() || c.workers == 0 || c.workers > 128 || c.queue == 0 || c.queue > 100000 ||
        c.max_connections == 0 || c.max_connections > 100000 || c.rotate == 0 ||
        c.rotate > 100000 || c.timeout_ms < 100 || c.heartbeat_ms < 10 ||
        c.suspect_ms < c.heartbeat_ms || c.delay_ms < 0)
        throw std::runtime_error("config_out_of_range");
    return c;
}
void log(const std::string &node, const std::string &level, const std::string &type,
         const std::string &context) {
    static std::mutex m;
    std::lock_guard lock(m);
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch())
                  .count();
    std::cerr << "timestamp_ms=" << ts << " level=" << level << " node=" << node
              << " event=" << type << " " << context << '\n';
}
} // namespace monitor
