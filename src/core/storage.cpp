#include "core.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
namespace monitor {
uint32_t checksum(const std::string &s) {
    uint32_t crc = ~0u;
    for (unsigned char b : s) {
        crc ^= b;
        for (int i = 0; i < 8; ++i)
            crc = (crc >> 1) ^ (0xedb88320u & (0u - (crc & 1u)));
    }
    return ~crc;
}
static void write_bytes(int fd, const void *data, size_t n) {
    auto p = static_cast<const char *>(data);
    while (n) {
        auto w = ::write(fd, p, n);
        if (w < 0 && errno == EINTR)
            continue;
        if (w <= 0)
            throw std::runtime_error("wal_write:" + std::string(std::strerror(errno)));
        p += w;
        n -= size_t(w);
    }
}
static void record(int fd, const Event &e) {
    auto s = e.encode();
    uint32_t h[] = {htonl(0x4d4f4e31), htonl(uint32_t(s.size())), htonl(checksum(s))};
    write_bytes(fd, h, sizeof(h));
    write_bytes(fd, s.data(), s.size());
}
static std::string read_file(int fd) {
    struct stat st{};
    if (fstat(fd, &st) < 0 || st.st_size > 128 * 1024 * 1024)
        throw std::runtime_error("wal_size_limit");
    std::string s(size_t(st.st_size), '\0');
    size_t n = 0;
    while (n < s.size()) {
        auto r = pread(fd, s.data() + n, s.size() - n, off_t(n));
        if (r < 0 && errno == EINTR)
            continue;
        if (r <= 0)
            throw std::runtime_error("wal_read");
        n += size_t(r);
    }
    return s;
}
Store::Store(std::filesystem::path path, bool durable, size_t rotate)
    : directory(std::move(path)), sync(durable), rotate_every(rotate) {
    std::filesystem::create_directories(directory);
    lock_file = Fd(open((directory / "LOCK").c_str(), O_CREAT | O_RDWR, 0600));
    if (lock_file.value < 0 || flock(lock_file.value, LOCK_EX | LOCK_NB) < 0)
        throw std::runtime_error("data_directory_locked");
    wal = Fd(open((directory / "events.wal").c_str(), O_CREAT | O_RDWR | O_APPEND, 0600));
    if (wal.value < 0)
        throw std::runtime_error("wal_open");
    if (sync) {
        Fd dir(open(directory.c_str(), O_RDONLY));
        if (dir.value < 0 || fsync(dir.value) < 0)
            throw std::runtime_error("directory_sync");
    }
    replay();
}
void Store::replay() {
    for (const auto &name : {"checkpoint", "events.wal"}) {
        Fd file(open((directory / name).c_str(), O_RDONLY));
        if (file.value < 0) {
            if (errno == ENOENT && std::string(name) == "checkpoint")
                continue;
            throw std::runtime_error("replay_open");
        }
        auto bytes = read_file(file.value);
        size_t offset = 0;
        while (offset < bytes.size()) {
            if (bytes.size() - offset < 12)
                break;
            uint32_t h[3];
            std::memcpy(h, bytes.data() + offset, 12);
            auto n = ntohl(h[1]);
            if (ntohl(h[0]) != 0x4d4f4e31 || n > frame_limit)
                throw std::runtime_error("corrupt_wal_header");
            if (bytes.size() - offset - 12 < n)
                break;
            auto line = bytes.substr(offset + 12, n);
            if (checksum(line) != ntohl(h[2]))
                throw std::runtime_error("corrupt_wal_checksum");
            auto e = Event::parse(line);
            auto it = state.find(e.device);
            if (it == state.end() || it->second.seq < e.seq)
                state[e.device] = e;
            if (state.size() > device_limit)
                throw std::runtime_error("device_capacity");
            offset += 12 + n;
            if (std::string(name) == "events.wal")
                ++writes;
        }
        if (offset != bytes.size()) {
            if (std::string(name) != "events.wal")
                throw std::runtime_error("incomplete_checkpoint");
            if (ftruncate(wal.value, off_t(offset)) < 0 || (sync && fsync(wal.value) < 0))
                throw std::runtime_error("wal_tail_truncate");
        }
    }
    if (writes >= rotate_every)
        checkpoint();
}
void Store::append(const Event &e) {
    record(wal.value, e);
    if (sync && fsync(wal.value) < 0)
        throw std::runtime_error("wal_sync");
}
void Store::checkpoint() {
    Fd out(open((directory / "checkpoint.tmp").c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600));
    if (out.value < 0)
        throw std::runtime_error("checkpoint_open");
    for (const auto &[id, e] : state) {
        (void)id;
        record(out.value, e);
    }
    if (sync && fsync(out.value) < 0)
        throw std::runtime_error("checkpoint_sync");
    if (rename((directory / "checkpoint.tmp").c_str(), (directory / "checkpoint").c_str()) < 0)
        throw std::runtime_error("checkpoint_rename");
    if (sync) {
        Fd dir(open(directory.c_str(), O_RDONLY));
        if (dir.value < 0 || fsync(dir.value) < 0)
            throw std::runtime_error("checkpoint_directory_sync");
    }
    if (ftruncate(wal.value, 0) < 0 || (sync && fsync(wal.value) < 0))
        throw std::runtime_error("checkpoint_truncate");
    writes = 0;
}
std::pair<Event, bool> Store::apply(const Event &e) {
    // ponytail: one storage lock serializes writes; batch/group commit if profiling justifies it.
    std::lock_guard lock(mu);
    if (poisoned)
        throw std::runtime_error("store_poisoned");
    auto it = state.find(e.device);
    if (it != state.end() && e.seq <= it->second.seq)
        return {it->second, false};
    if (it == state.end() && state.size() >= device_limit)
        throw std::runtime_error("device_capacity");
    try {
        append(e);
        state[e.device] = e;
        if (++writes >= rotate_every)
            checkpoint();
    } catch (...) {
        poisoned = true;
        throw;
    }
    return {e, true};
}
std::string Store::snapshot() const {
    std::lock_guard lock(mu);
    if (poisoned)
        throw std::runtime_error("store_poisoned");
    std::string s = "SNAP";
    for (const auto &[id, e] : state) {
        (void)id;
        s += '|';
        s += e.encode();
    }
    if (s.size() > snapshot_limit)
        throw std::runtime_error("snapshot_capacity");
    return s;
}
void Store::install(const std::string &snap) {
    auto lines = split(snap, '|');
    if (lines.empty() || lines[0] != "SNAP" || snap.size() > snapshot_limit)
        throw std::runtime_error("invalid_snapshot");
    std::map<std::string, Event> next;
    for (size_t i = 1; i < lines.size(); ++i) {
        auto e = Event::parse(lines[i]);
        if (!next.emplace(e.device, e).second || next.size() > device_limit)
            throw std::runtime_error("invalid_snapshot_devices");
    }
    std::lock_guard lock(mu);
    if (poisoned)
        throw std::runtime_error("store_poisoned");
    for (const auto &[id, e] : state) {
        auto it = next.find(id);
        if (it == next.end() || it->second.seq < e.seq ||
            (it->second.seq == e.seq && it->second.encode() != e.encode()))
            throw std::runtime_error("leader_state_regressed");
    }
    if (state.size() == next.size()) {
        bool equal = true;
        for (auto &[id, e] : next)
            if (state.at(id).encode() != e.encode()) {
                equal = false;
                break;
            }
        if (equal)
            return;
    }
    state = std::move(next);
    try {
        checkpoint();
    } catch (...) {
        poisoned = true;
        throw;
    }
}
std::optional<Event> Store::get(const std::string &id) const {
    std::lock_guard lock(mu);
    if (poisoned)
        throw std::runtime_error("store_poisoned");
    auto it = state.find(id);
    if (it == state.end())
        return {};
    return it->second;
}
} // namespace monitor
