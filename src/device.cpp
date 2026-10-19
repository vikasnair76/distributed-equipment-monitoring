#include "core.hpp"
#include <csignal>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <random>
#include <sys/file.h>
namespace {
volatile sig_atomic_t stop_device = 0;
void stop(int) {
    stop_device = 1;
}
void persist(const std::filesystem::path &path, uint64_t next, uint64_t command_seq,
             const std::string &command) {
    auto tmp = path;
    tmp += ".tmp";
    monitor::Fd fd(open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600));
    if (fd.value < 0)
        throw std::runtime_error("device_state_open");
    std::string data =
        std::to_string(next) + " " + std::to_string(command_seq) + " " + command + "\n";
    size_t n = 0;
    while (n < data.size()) {
        auto r = write(fd.value, data.data() + n, data.size() - n);
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            throw std::runtime_error("device_state_write");
        n += size_t(r);
    }
    if (fsync(fd.value) < 0 || rename(tmp.c_str(), path.c_str()) < 0)
        throw std::runtime_error("device_state_sync");
    auto dir = path.parent_path();
    if (dir.empty())
        dir = ".";
    monitor::Fd parent(open(dir.c_str(), O_RDONLY));
    if (parent.value < 0 || fsync(parent.value) < 0)
        throw std::runtime_error("device_directory_sync");
}
} // namespace
int main(int argc, char **argv) {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, stop);
    std::signal(SIGTERM, stop);
    try {
        if (argc < 4)
            throw std::runtime_error("usage: device ID endpoint[,endpoint] STATE_FILE "
                                     "[interval_ms=2000] [event_count=0]");
        std::string id = argv[1];
        monitor::Event::parse("TELE " + id + " 1 70 30 2000 RUNNING");
        auto endpoints = monitor::split(argv[2], ',');
        if (endpoints.empty())
            throw std::runtime_error("no_endpoints");
        std::filesystem::path path = argv[3];
        if (!path.parent_path().empty())
            std::filesystem::create_directories(path.parent_path());
        monitor::Fd lock(open((path.string() + ".lock").c_str(), O_CREAT | O_RDWR, 0600));
        if (lock.value < 0 || flock(lock.value, LOCK_EX | LOCK_NB) < 0)
            throw std::runtime_error("device_state_locked");
        uint64_t next = 1, command_seq = 0;
        std::string command = "HOLD";
        if (std::filesystem::exists(path)) {
            std::ifstream f(path);
            if (!(f >> next >> command_seq >> command) || next <= command_seq ||
                (command != "HOLD" && command != "SHUTDOWN" && command != "SLOW_DOWN" &&
                 command != "RESUME_NORMAL"))
                throw std::runtime_error("invalid_device_state");
        }
        int interval = argc > 4 ? std::stoi(argv[4]) : 2000;
        uint64_t count = argc > 5 ? std::stoull(argv[5]) : 0;
        if (interval < 1)
            throw std::runtime_error("invalid_interval");
        std::mt19937 rng(std::random_device{}());
        monitor::Fd socket;
        size_t endpoint = 0;
        int backoff = 50;
        uint64_t sent = 0;
        bool previously_connected = false;
        while (!stop_device && command != "SHUTDOWN" && (!count || sent < count)) {
            monitor::Event event{id,
                                 "RUNNING",
                                 next++,
                                 70.0 + double(rng() % 270) / 10.0,
                                 30.0 + double(rng() % 170) / 10.0,
                                 2000 + int(rng() % 1800)};
            persist(path, next, command_seq, command);
            auto line = event.encode();
            while (!stop_device) {
                try {
                    if (socket.value < 0) {
                        socket = monitor::connect_to(endpoints[endpoint], 500);
                        if (previously_connected) {
                            if (!monitor::send_all(socket.value, "FAILOVER " + id + '\n') ||
                                monitor::read_line(socket.value) != "OK")
                                throw std::runtime_error("failover_report_failed");
                        }
                        previously_connected = true;
                        monitor::log(id, "INFO", "connected", "endpoint=" + endpoints[endpoint]);
                    }
                    if (!monitor::send_all(socket.value, line + '\n'))
                        throw std::runtime_error("send_failed");
                    auto response = monitor::read_line(socket.value);
                    std::istringstream in(response);
                    std::string tag, device, action;
                    uint64_t seq = 0;
                    if (!(in >> tag >> device >> seq >> action) || tag != "ACK" || device != id ||
                        seq < event.seq ||
                        (action != "SLOW_DOWN" && action != "RESUME_NORMAL" &&
                         action != "SHUTDOWN"))
                        throw std::runtime_error("uncommitted_response");
                    next = std::max(next, seq + 1);
                    if (seq > command_seq) {
                        persist(path, next, seq, action);
                        command_seq = seq;
                        command = action;
                        monitor::log(id, "INFO", "command_applied",
                                     "command=" + command + " sequence=" + std::to_string(seq));
                    }
                    if (!monitor::send_all(socket.value,
                                           "CACK " + id + ' ' + std::to_string(seq) + '\n') ||
                        monitor::read_line(socket.value) != "OK")
                        throw std::runtime_error("command_ack_retry");
                    ++sent;
                    backoff = 50;
                    break;
                } catch (const std::exception &e) {
                    socket = monitor::Fd();
                    endpoint = (endpoint + 1) % endpoints.size();
                    monitor::log(id, "WARNING", "failover_hold", std::string("error=") + e.what());
                    int wait = backoff + int(rng() % unsigned(backoff / 2 + 1));
                    for (int t = 0; t < wait && !stop_device; t += 20)
                        std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    backoff = std::min(2000, backoff * 2);
                }
            }
            int wait = command == "SLOW_DOWN" ? interval * 5 / 2 : interval;
            for (int elapsed = 0; elapsed < wait && !stop_device && command != "SHUTDOWN";
                 elapsed += 20)
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "fatal: " << e.what() << '\n';
        return 1;
    }
}
