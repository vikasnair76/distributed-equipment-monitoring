#include "core.hpp"
#include <cmath>
#include <csignal>
#include <iostream>
#include <random>
int main(int argc, char **argv) {
    std::signal(SIGPIPE, SIG_IGN);
    try {
        if (argc < 2)
            throw std::runtime_error("usage: load_generator endpoint[,endpoint] [clients=100] "
                                     "[seconds=10] [hz=1] [padding=0] [burst=1]");
        auto endpoints = monitor::split(argv[1], ',');
        size_t clients = argc > 2 ? std::stoul(argv[2]) : 100;
        int seconds = argc > 3 ? std::stoi(argv[3]) : 10;
        double hz = argc > 4 ? std::stod(argv[4]) : 1;
        size_t padding = argc > 5 ? std::stoul(argv[5]) : 0;
        size_t burst = argc > 6 ? std::stoul(argv[6]) : 1;
        if (clients < 1 || clients > 10000 || seconds < 1 || !std::isfinite(hz) || hz <= 0 ||
            padding > 3000 || burst < 1 || burst > 1000)
            throw std::runtime_error("invalid_workload");
        std::mutex mu;
        std::vector<double> latency, reconnect;
        std::atomic<uint64_t> rejected{0}, attempts{0};
        std::vector<std::jthread> threads;
        auto start = monitor::Clock::now(), end = start + std::chrono::seconds(seconds);
        auto run = std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        for (size_t i = 0; i < clients; ++i)
            threads.emplace_back([&, i] {
                monitor::Fd fd;
                size_t ep = i % endpoints.size();
                uint64_t seq = 1;
                monitor::Clock::time_point outage_start{};
                bool in_outage = false;
                auto due = start;
                while (monitor::Clock::now() < end) {
                    auto now = monitor::Clock::now();
                    if (now < due) {
                        std::this_thread::sleep_until(std::min(due, end));
                        continue;
                    }
                    due = now + std::chrono::duration_cast<monitor::Clock::duration>(
                                    std::chrono::duration<double>(double(burst) / hz));
                    for (size_t b = 0; b < burst && monitor::Clock::now() < end; ++b) {
                        monitor::Event event{
                            "load_" + run + '_' + std::to_string(i), "RUNNING", seq, 70, 30, 2000};
                        auto t = monitor::Clock::now();
                        ++attempts;
                        try {
                            if (fd.value < 0)
                                fd = monitor::connect_to(endpoints[ep], 300);
                            auto message = event.encode();
                            if (padding)
                                message += ' ' + std::string(padding, 'x');
                            if (!monitor::send_all(fd.value, message + '\n', 500))
                                throw std::runtime_error("send");
                            auto answer = monitor::read_line(fd.value, monitor::frame_limit, 1000);
                            if (answer.rfind(
                                    "ACK " + event.device + ' ' + std::to_string(seq) + ' ', 0) !=
                                0)
                                throw std::runtime_error("rejected");
                            ++seq;
                            std::lock_guard lock(mu);
                            latency.push_back(
                                std::chrono::duration<double, std::milli>(monitor::Clock::now() - t)
                                    .count());
                            if (in_outage) {
                                reconnect.push_back(std::chrono::duration<double, std::milli>(
                                                        monitor::Clock::now() - outage_start)
                                                        .count());
                                in_outage = false;
                            }
                        } catch (...) {
                            ++rejected;
                            fd = monitor::Fd();
                            ep = (ep + 1) % endpoints.size();
                            if (!in_outage) {
                                outage_start = monitor::Clock::now();
                                in_outage = true;
                            }
                            std::this_thread::sleep_for(std::chrono::milliseconds(50));
                        }
                    }
                }
            });
        for (auto &t : threads)
            t.join();
        double elapsed = std::chrono::duration<double>(monitor::Clock::now() - start).count();
        std::sort(latency.begin(), latency.end());
        std::sort(reconnect.begin(), reconnect.end());
        auto quantile = [](const std::vector<double> &v, double p) {
            return v.empty() ? 0.0 : v[std::min(v.size() - 1, size_t(p * double(v.size() - 1)))];
        };
        std::cout << "{\"clients\":" << clients << ",\"elapsed_seconds\":" << elapsed
                  << ",\"attempts\":" << attempts << ",\"acknowledged\":" << latency.size()
                  << ",\"rejected_or_failed_attempts\":" << rejected
                  << ",\"events_per_second\":" << double(latency.size()) / elapsed
                  << ",\"p50_ms\":" << quantile(latency, .5)
                  << ",\"p95_ms\":" << quantile(latency, .95)
                  << ",\"p99_ms\":" << quantile(latency, .99)
                  << ",\"reconnect_samples\":" << reconnect.size()
                  << ",\"reconnect_p95_ms\":" << quantile(reconnect, .95) << "}\n";
        return latency.empty() ? 2 : 0;
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
