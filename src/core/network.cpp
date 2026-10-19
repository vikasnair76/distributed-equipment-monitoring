#include "core.hpp"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
namespace monitor {
static bool ready(int fd, short events, Clock::time_point deadline) {
    for (;;) {
        auto left =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (left <= 0)
            return false;
        pollfd p{fd, events, 0};
        auto n = poll(&p, 1, int(left));
        if (n < 0 && errno == EINTR)
            continue;
        return n > 0 && (p.revents & events);
    }
}
Fd connect_to(const std::string &endpoint, int timeout_ms) {
    auto pos = endpoint.rfind(':');
    if (pos == std::string::npos)
        throw std::runtime_error("invalid_endpoint");
    auto host = endpoint.substr(0, pos), port = endpoint.substr(pos + 1);
    addrinfo hints{}, *list = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &list) != 0)
        throw std::runtime_error("resolve_failed");
    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> owner(list, freeaddrinfo);
    for (auto p = list; p; p = p->ai_next) {
        Fd fd(socket(p->ai_family, p->ai_socktype, p->ai_protocol));
        if (fd.value < 0)
            continue;
        if (fcntl(fd.value, F_SETFL, O_NONBLOCK) < 0)
            continue;
        int r = connect(fd.value, p->ai_addr, p->ai_addrlen);
        if (r < 0 && errno != EINPROGRESS)
            continue;
        if (r < 0 &&
            !ready(fd.value, POLLOUT, Clock::now() + std::chrono::milliseconds(timeout_ms)))
            continue;
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(fd.value, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error)
            continue;
        return fd;
    }
    throw std::runtime_error("connect_failed:" + endpoint);
}
bool send_all(int fd, const std::string &s, int timeout_ms) {
    auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    size_t n = 0;
    while (n < s.size()) {
        auto w = send(fd, s.data() + n, s.size() - n, 0);
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!ready(fd, POLLOUT, deadline))
                return false;
            continue;
        }
        if (w <= 0)
            return false;
        n += size_t(w);
    }
    return true;
}
std::string read_line(int fd, size_t limit, int timeout_ms) {
    auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    std::string s;
    char b[8192];
    while (s.size() <= limit) {
        auto r = recv(fd, b, std::min(sizeof(b), limit + 1 - s.size()), 0);
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!ready(fd, POLLIN, deadline))
                throw std::runtime_error("read_timeout");
            continue;
        }
        if (r <= 0)
            throw std::runtime_error("disconnected");
        s.append(b, size_t(r));
        auto pos = s.find('\n');
        if (pos != std::string::npos) {
            if (pos > limit)
                break;
            return s.substr(0, pos);
        }
    }
    throw std::runtime_error("response_too_large");
}
std::string rpc(const std::string &address, const std::string &request, size_t limit,
                int timeout_ms) {
    auto fd = connect_to(address, timeout_ms);
    if (!send_all(fd.value, request + '\n', timeout_ms))
        throw std::runtime_error("send_failed");
    return read_line(fd.value, limit, timeout_ms);
}
} // namespace monitor
