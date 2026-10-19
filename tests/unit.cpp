#include "core.hpp"
#include <fstream>
#include <iostream>
#define CHECK(x)                                                                                   \
    do {                                                                                           \
        if (!(x))                                                                                  \
            throw std::runtime_error("check failed: " #x);                                         \
    } while (false)
using namespace monitor;
int main() {
    try {
        auto path =
            std::filesystem::temp_directory_path() / ("monitor-unit-" + std::to_string(getpid()));
        std::filesystem::remove_all(path);
        Event e = Event::parse("TELE machine 1 70 30 2000 RUNNING");
        CHECK(e.command() == "RESUME_NORMAL");
        CHECK(checksum("123456789") == 0xcbf43926);
        for (auto line :
             {"TELE x 0 70 30 2 RUNNING", "TELE x -2 70 30 2 RUNNING", "TELE x 1 nan 30 2 RUNNING",
              "TELE x 1 70 30 2 BAD", "TELE x 1 70 30 2 RUNNING rubbish",
              "TELE x 1 70 30 2 RUNNING xxx extra"}) {
            bool threw = false;
            try {
                Event::parse(line);
            } catch (...) {
                threw = true;
            }
            CHECK(threw);
        }
        e.temperature = 95;
        CHECK(e.command() == "SLOW_DOWN");
        e.pressure = 50;
        CHECK(e.command() == "SHUTDOWN");
        e.pressure = 30;
        e.temperature = 70;
        {
            Store s(path, true, 2);
            CHECK(s.apply(e).second);
            CHECK(!s.apply(e).second);
            e.seq = 2;
            CHECK(s.apply(e).second);
            e.seq = 1;
            CHECK(!s.apply(e).second);
            CHECK(s.get("machine")->seq == 2);
            bool locked = false;
            try {
                Store second(path);
            } catch (...) {
                locked = true;
            }
            CHECK(locked);
        }
        {
            std::ofstream f(path / "events.wal", std::ios::app | std::ios::binary);
            f << "partial";
        }
        {
            Store s(path);
            CHECK(s.get("machine")->seq == 2);
            CHECK(std::filesystem::file_size(path / "events.wal") == 0);
            e.seq = 3;
            CHECK(s.apply(e).second);
            auto snap = s.snapshot();
            {
                Store replica(path / "replica");
                replica.install(snap);
                CHECK(replica.get("machine")->seq == 3);
                bool conflicting = false;
                try {
                    replica.install("SNAP|TELE machine 3 80 30 2000 RUNNING");
                } catch (...) {
                    conflicting = true;
                }
                CHECK(conflicting);
                bool regressed = false;
                try {
                    replica.install("SNAP");
                } catch (...) {
                    regressed = true;
                }
                CHECK(regressed);
            }
        }
        {
            std::fstream f(path / "events.wal", std::ios::in | std::ios::out | std::ios::binary);
            f.seekp(13);
            f.put('!');
        }
        bool corrupt = false;
        try {
            Store s(path);
        } catch (...) {
            corrupt = true;
        }
        CHECK(corrupt);
        // Rotation budget includes records replayed over process restarts.
        {
            Store s(path / "restart", true, 3);
            e.seq = 1;
            s.apply(e);
            e.seq = 2;
            s.apply(e);
        }
        {
            Store s(path / "restart", true, 3);
            e.seq = 3;
            s.apply(e);
            CHECK(std::filesystem::file_size(path / "restart" / "events.wal") == 0);
        }
        std::mutex mu;
        std::condition_variable cv;
        bool release = false;
        std::atomic<bool> entered{false};
        std::atomic<int> ran{0};
        {
            Pool pool(1, 1);
            CHECK(pool.submit([&] {
                entered = true;
                std::unique_lock l(mu);
                cv.wait(l, [&] { return release; });
                ++ran;
            }));
            while (!entered)
                std::this_thread::yield();
            CHECK(pool.submit([&] { ++ran; }));
            CHECK(!pool.submit([] {}));
            CHECK(pool.depth() == 1);
            {
                std::lock_guard l(mu);
                release = true;
            }
            cv.notify_all();
            pool.stop();
            CHECK(ran == 2);
            CHECK(!pool.submit([] {}));
        }
        std::filesystem::remove_all(path);
        std::cout << "unit: parser, thresholds, CRC, replay, rotation, tail repair, corruption "
                     "rejection, locking, snapshot regression, bounded pool PASS\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
