#include "core.hpp"
#include <arpa/inet.h>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <poll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#ifdef __linux__
#include <sys/epoll.h>
#endif
namespace monitor {
static volatile sig_atomic_t stopping = 0;
static void signal_stop(int) {
    stopping = 1;
}
struct Metrics {
    std::atomic<uint64_t> received{0}, processed{0}, invalid{0}, duplicate{0}, wal_errors{0},
        rejected{0}, commands{0}, forwarded{0}, failovers{0}, timeouts{0};
    std::atomic<size_t> active{0}, connected{0};
    std::atomic<uint64_t> latency[6]{};
    std::atomic<double> latency_sum{0};
    Clock::time_point start = Clock::now();
    void observe(double seconds) {
        latency_sum.fetch_add(seconds);
        const double bounds[] = {.001, .005, .01, .05, .1, 1e100};
        for (int i = 0; i < 6; ++i)
            if (seconds <= bounds[i])
                ++latency[i];
    }
    std::string render(size_t depth) {
        std::ostringstream o;
        rusage usage{};
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            double cpu = double(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
                         double(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1000000.0;
#ifdef __APPLE__
            auto memory_bytes = usage.ru_maxrss;
#else
            auto memory_bytes = usage.ru_maxrss * 1024;
#endif
            o << "monitor_process_cpu_seconds_total " << cpu << '\n'
              << "monitor_process_max_rss_bytes " << memory_bytes << '\n';
        }
        o << "monitor_active_connections " << active << "\nmonitor_total_events_received "
          << received << "\nmonitor_total_events_processed " << processed
          << "\nmonitor_invalid_events_total " << invalid << "\nmonitor_duplicate_events_total "
          << duplicate << "\nmonitor_queue_depth " << depth << "\nmonitor_connected_nodes "
          << connected << "\nmonitor_failover_events_total " << failovers
          << "\nmonitor_forwarded_requests_total " << forwarded
          << "\nmonitor_wal_write_errors_total " << wal_errors << "\nmonitor_rejected_events_total "
          << rejected << "\nmonitor_command_acks_total " << commands
          << "\nmonitor_membership_timeouts_total " << timeouts
          << "\nmonitor_process_uptime_seconds "
          << std::chrono::duration<double>(Clock::now() - start).count() << '\n';
        const char *bounds[] = {"0.001", "0.005", "0.01", "0.05", "0.1", "+Inf"};
        o << "# TYPE monitor_processing_seconds histogram\n";
        for (int i = 0; i < 6; ++i)
            o << "monitor_processing_seconds_bucket{le=\"" << bounds[i] << "\"} " << latency[i]
              << '\n';
        o << "monitor_processing_seconds_count " << latency[5] << '\n'
          << "monitor_processing_seconds_sum " << latency_sum << '\n';
        return o.str();
    }
};
struct Connection {
    Fd fd;
    uint64_t id;
    std::string input, output;
    size_t sent = 0;
    bool busy = false, close_after = false;
    Clock::time_point touched = Clock::now();
};
struct Completion {
    uint64_t id;
    std::string response;
    bool close_after;
};
class Server {
    Config c;
    Store store;
    Metrics metrics;
    Pool pool;
    Fd listener;
    std::map<uint64_t, Connection> connections;
    uint64_t next_id = 1;
    std::mutex done_mu;
    std::deque<Completion> done;
    std::atomic<size_t> response_bytes{0};
    std::atomic<bool> background_stop{false};
    std::thread background;
#ifdef __linux__
    Fd epoll_fd;
    void watch(Connection &conn) {
        epoll_event ev{};
        ev.data.u64 = conn.id;
        ev.events = EPOLLRDHUP;
        if (!conn.busy && conn.output.empty() && conn.input.find('\n') == std::string::npos)
            ev.events |= EPOLLIN;
        if (!conn.output.empty())
            ev.events |= EPOLLOUT;
        if (epoll_ctl(epoll_fd.value, EPOLL_CTL_MOD, conn.fd.value, &ev) < 0)
            throw std::runtime_error("epoll_mod");
    }
#else
    void watch(Connection &) {}
#endif
    std::string handle(const std::string &line, bool &close_after) {
        if (c.delay_ms)
            std::this_thread::sleep_for(std::chrono::milliseconds(c.delay_ms));
        if (line.rfind("GET /metrics ", 0) == 0 || line.rfind("GET /health ", 0) == 0) {
            close_after = true;
            auto body = line.rfind("GET /metrics ", 0) == 0 ? metrics.render(pool.depth()) : "ok\n";
            return "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nConnection: "
                   "close\r\nContent-Length: " +
                   std::to_string(body.size()) + "\r\n\r\n" + body;
        }
        if (line.rfind("FAILOVER ", 0) == 0) {
            auto id = line.substr(9);
            try {
                Event::parse("TELE " + id + " 1 70 30 2000 RUNNING");
            } catch (...) {
                return "ERR invalid_device\n";
            }
            ++metrics.failovers;
            log(c.id, "INFO", "device_failover", "device=" + id);
            return "OK\n";
        }
        if (line == "PING")
            return "PONG " + c.id + '\n';
        if (line == "SNAPSHOT") {
            if (c.listen != c.leader)
                return "ERR not_leader\n";
            return store.snapshot() + '\n';
        }
        if (line.rfind("STATE ", 0) == 0) {
            auto e = store.get(line.substr(6));
            return e ? e->encode() + '\n' : "MISSING\n";
        }
        if (line.rfind("CACK ", 0) == 0) {
            if (c.listen != c.leader) {
                try {
                    return rpc(c.leader, line) + '\n';
                } catch (...) {
                    return "RETRY leader_unavailable\n";
                }
            }
            std::istringstream in(line);
            std::string tag, id, extra;
            uint64_t seq = 0;
            if (!(in >> tag >> id >> seq) || (in >> extra))
                return "ERR invalid_command_ack\n";
            auto e = store.get(id);
            if (e && e->seq == seq) {
                ++metrics.commands;
                return "OK\n";
            }
            return "ERR stale_command_ack\n";
        }
        ++metrics.received;
        Event e;
        try {
            e = Event::parse(line);
        } catch (...) {
            ++metrics.invalid;
            return "ERR invalid_event\n";
        }
        if (c.listen != c.leader) {
            try {
                auto answer = rpc(c.leader, e.encode());
                ++metrics.forwarded;
                return answer + '\n';
            } catch (const std::exception &) {
                return "RETRY leader_unavailable\n";
            }
        }
        try {
            auto [current, changed] = store.apply(e);
            if (changed)
                ++metrics.processed;
            else
                ++metrics.duplicate;
            if (changed && current.command() != "RESUME_NORMAL")
                log(c.id, "WARNING", "equipment_alert",
                    "device=" + current.device + " sequence=" + std::to_string(current.seq) +
                        " command=" + current.command());
            return "ACK " + current.device + ' ' + std::to_string(current.seq) + ' ' +
                   current.command() + '\n';
        } catch (const std::exception &error) {
            ++metrics.wal_errors;
            log(c.id, "ERROR", "persistence_failure",
                "device=" + e.device + " error=" + error.what());
            return "RETRY persistence_failure\n";
        }
    }
    void cluster() {
        std::map<std::string, Clock::time_point> last;
        std::map<std::string, bool> alive;
        for (auto &peer : c.peers)
            last[peer] = Clock::now();
        while (!background_stop) {
            size_t count = 0;
            for (auto &peer : c.peers) {
                if (background_stop)
                    break;
                bool ok = false;
                try {
                    auto s = rpc(peer, "PING", frame_limit, 300);
                    ok = s.rfind("PONG ", 0) == 0;
                } catch (...) {
                }
                if (ok) {
                    last[peer] = Clock::now();
                    if (!alive[peer])
                        log(c.id, "INFO", "peer_recovered", "peer=" + peer);
                    alive[peer] = true;
                } else if (Clock::now() - last[peer] > std::chrono::milliseconds(c.suspect_ms) &&
                           alive[peer]) {
                    alive[peer] = false;
                    ++metrics.timeouts;
                    log(c.id, "WARNING", "peer_suspected", "peer=" + peer);
                }
                if (alive[peer])
                    ++count;
            }
            metrics.connected = count;
            if (c.listen != c.leader && !background_stop) {
                try {
                    auto snapshot = rpc(c.leader, "SNAPSHOT", snapshot_limit, 2000);
                    store.install(snapshot);
                } catch (const std::exception &e) {
                    log(c.id, "WARNING", "replication_retry", std::string("error=") + e.what());
                }
            }
            for (int elapsed = 0; elapsed < c.heartbeat_ms && !background_stop; elapsed += 20)
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    void accept_clients() {
        for (size_t accepted = 0; accepted < 64; ++accepted) {
            Fd fd(accept(listener.value, nullptr, nullptr));
            if (fd.value < 0) {
                if (errno == EINTR)
                    continue;
                return;
            }
            if (connections.size() >= c.max_connections) {
                ++metrics.rejected;
                continue;
            }
            if (fcntl(fd.value, F_SETFL, O_NONBLOCK) < 0)
                continue;
            uint64_t id = next_id++;
#ifdef __linux__
            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLRDHUP;
            ev.data.u64 = id;
            if (epoll_ctl(epoll_fd.value, EPOLL_CTL_ADD, fd.value, &ev) < 0)
                continue;
#endif
            Connection conn{std::move(fd), id, {}, {}};
            connections.emplace(id, std::move(conn));
            metrics.active = connections.size();
        }
    }
    void remove(uint64_t id) {
#ifdef __linux__
        auto it = connections.find(id);
        if (it != connections.end())
            epoll_ctl(epoll_fd.value, EPOLL_CTL_DEL, it->second.fd.value, nullptr);
#endif
        auto found = connections.find(id);
        if (found != connections.end())
            response_bytes -= found->second.output.size();
        connections.erase(id);
        metrics.active = connections.size();
    }
    void io(uint64_t id, bool readable, bool writable, bool error) {
        auto it = connections.find(id);
        if (it == connections.end())
            return;
        auto &conn = it->second;
        if (error) {
            remove(id);
            return;
        }
        if (readable && !conn.busy && conn.output.empty()) {
            char b[4096];
            auto n =
                recv(conn.fd.value, b, std::min(sizeof(b), frame_limit + 1 - conn.input.size()), 0);
            if (n == 0) {
                remove(id);
                return;
            }
            if (n < 0) {
                if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
                    remove(id);
                return;
            }
            conn.input.append(b, size_t(n));
            conn.touched = Clock::now();
            if (conn.input.size() > frame_limit && conn.input.find('\n') == std::string::npos) {
                remove(id);
                return;
            }
        }
        if (writable && !conn.output.empty()) {
            auto n = send(conn.fd.value, conn.output.data() + conn.sent,
                          conn.output.size() - conn.sent, 0);
            if (n < 0) {
                if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
                    remove(id);
                return;
            }
            if (n == 0) {
                remove(id);
                return;
            }
            conn.sent += size_t(n);
            conn.touched = Clock::now();
            if (conn.sent == conn.output.size()) {
                response_bytes -= conn.output.size();
                conn.output.clear();
                conn.sent = 0;
                if (conn.close_after) {
                    remove(id);
                    return;
                }
            }
        }
        watch(conn);
    }
    void dispatch() {
        std::deque<Completion> ready;
        {
            std::lock_guard lock(done_mu);
            ready.swap(done);
        }
        for (auto &completion : ready) {
            auto it = connections.find(completion.id);
            if (it == connections.end()) {
                response_bytes -= completion.response.size();
                continue;
            }
            auto &conn = it->second;
            conn.busy = false;
            conn.output = std::move(completion.response);
            conn.close_after = completion.close_after;
            watch(conn);
        }
        std::vector<uint64_t> expired;
        for (auto &[id, conn] : connections) {
            if (Clock::now() - conn.touched > std::chrono::milliseconds(c.timeout_ms)) {
                expired.push_back(id);
                continue;
            }
            if (conn.busy || !conn.output.empty())
                continue;
            auto pos = conn.input.find('\n');
            if (pos == std::string::npos)
                continue;
            auto line = conn.input.substr(0, pos);
            if (!line.empty() && line.back() == '\r')
                line.pop_back();
            bool admitted = pool.submit([this, id, line = std::move(line), start = Clock::now()] {
                bool close_after = false;
                std::string response;
                try {
                    response = handle(line, close_after);
                } catch (const std::exception &e) {
                    response = "ERR internal\n";
                    log(c.id, "ERROR", "request_failure", std::string("error=") + e.what());
                }
                metrics.observe(std::chrono::duration<double>(Clock::now() - start).count());
                std::lock_guard lock(done_mu);
                if (response_bytes.load() + response.size() > 16 * 1024 * 1024) {
                    response = "RETRY response_capacity\n";
                    close_after = true;
                }
                response_bytes += response.size();
                done.push_back({id, std::move(response), close_after});
            });
            if (admitted) {
                conn.input.erase(0, pos + 1);
                conn.busy = true;
                watch(conn);
            } // Full queue: retain one bounded frame and retry; TCP provides backpressure.
        }
        for (auto id : expired)
            remove(id);
    }

  public:
    explicit Server(Config cfg)
        : c(std::move(cfg)), store(c.data, c.sync, c.rotate), pool(c.workers, c.queue) {
        auto p = c.listen.rfind(':');
        if (p == std::string::npos)
            throw std::runtime_error("invalid_listen");
        int port = std::stoi(c.listen.substr(p + 1));
        if (port < 1 || port > 65535)
            throw std::runtime_error("invalid_port");
        listener = Fd(socket(AF_INET, SOCK_STREAM, 0));
        if (listener.value < 0)
            throw std::runtime_error("listen_socket");
        int yes = 1;
        setsockopt(listener.value, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(uint16_t(port));
        if (bind(listener.value, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0 ||
            listen(listener.value, 1024) < 0 || fcntl(listener.value, F_SETFL, O_NONBLOCK) < 0)
            throw std::runtime_error("listen_failed:" + std::string(std::strerror(errno)));
#ifdef __linux__
        epoll_fd = Fd(epoll_create1(EPOLL_CLOEXEC));
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.u64 = 0;
        if (epoll_fd.value < 0 || epoll_ctl(epoll_fd.value, EPOLL_CTL_ADD, listener.value, &ev) < 0)
            throw std::runtime_error("epoll_create");
#endif
        background = std::thread([this] { cluster(); });
    }
    ~Server() {
        background_stop = true;
        if (background.joinable())
            background.join();
        pool.stop();
    }
    void run() {
        log(c.id, "INFO", "started", "listen=" + c.listen + " leader=" + c.leader);
        while (!stopping) {
            dispatch();
#ifdef __linux__
            epoll_event events[128];
            int n = epoll_wait(epoll_fd.value, events, 128, 5);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                throw std::runtime_error("epoll_wait");
            }
            for (int i = 0; i < n; ++i) {
                auto id = events[i].data.u64;
                auto mask = events[i].events;
                if (id == 0)
                    accept_clients();
                else
                    io(id, mask & EPOLLIN, mask & EPOLLOUT,
                       mask & (EPOLLERR | EPOLLHUP | EPOLLRDHUP));
            }
#else
            std::vector<pollfd> fds{{listener.value, POLLIN, 0}};
            std::vector<uint64_t> ids{0};
            for (auto &[id, conn] : connections) {
                short events = 0;
                if (!conn.busy && conn.output.empty() && conn.input.find('\n') == std::string::npos)
                    events = POLLIN;
                if (!conn.output.empty())
                    events |= POLLOUT;
                fds.push_back({conn.fd.value, events, 0});
                ids.push_back(id);
            }
            int n = poll(fds.data(), fds.size(), 5);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                throw std::runtime_error("poll");
            }
            for (size_t i = 0; i < fds.size(); ++i)
                if (fds[i].revents) {
                    if (i == 0)
                        accept_clients();
                    else
                        io(ids[i], fds[i].revents & POLLIN, fds[i].revents & POLLOUT,
                           fds[i].revents & (POLLERR | POLLHUP | POLLNVAL));
                }
#endif
        }
        log(c.id, "INFO", "draining");
        background_stop = true;
        if (background.joinable())
            background.join();
        pool.stop();
        dispatch();
        // Accepted jobs are durable before shutdown; clients retry any response lost while sockets
        // close.
    }
};
void serve(const Config &c) {
    Server server(c);
    server.run();
}
} // namespace monitor
int main(int argc, char **argv) {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGTERM, monitor::signal_stop);
    std::signal(SIGINT, monitor::signal_stop);
    try {
        if (argc != 2)
            throw std::runtime_error("usage: server CONFIG");
        monitor::serve(monitor::Config::load(argv[1]));
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "fatal: " << e.what() << '\n';
        return 1;
    }
}
