#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

class Socket {
public:
    explicit Socket(int fd = -1) : fd_(fd) {}
    ~Socket() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }

    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) {
                close(fd_);
            }
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    int fd() const { return fd_; }

private:
    int fd_{-1};
};

std::optional<Socket> connectToServer(const std::string& host, int port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "socket failed: " << std::strerror(errno) << "\n";
        return std::nullopt;
    }

    // Without a receive/send timeout, a peer that accepts the connection
    // and then goes silent (never responds, never closes) leaves recv()
    // blocked forever -- confirmed live: a GET_STATUS call this tool made
    // hung for 40+ hours straight on Fly.io, and since the calling
    // process never exits, run-jobs' own don't-die-on-failure retry logic
    // (which only helps once a call actually returns) never got a chance
    // to kick in either. Ported from frontier_main.cpp's connectToNode(),
    // which already has this -- SO_RCVTIMEO/SO_SNDTIMEO alone don't
    // reliably bound connect() itself on a blocking socket (confirmed:
    // an earlier version of this fix that only set those two options hit
    // spurious "connect failed: Operation now in progress"), hence the
    // nonblocking-connect + select() timeout below for the connect phase
    // specifically; the socket is restored to blocking afterward so the
    // SO_RCVTIMEO/SO_SNDTIMEO above apply normally to send()/recv().
    timeval timeout{};
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<std::uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        std::cerr << "invalid IPv4 address: " << host << "\n";
        close(fd);
        return std::nullopt;
    }

    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        std::cerr << "could not configure nonblocking connect: " << std::strerror(errno) << "\n";
        close(fd);
        return std::nullopt;
    }

    const int connected = connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (connected != 0 && errno != EINPROGRESS) {
        std::cerr << "connect failed: " << std::strerror(errno) << "\n";
        close(fd);
        return std::nullopt;
    }

    if (connected != 0) {
        fd_set write_set;
        FD_ZERO(&write_set);
        FD_SET(fd, &write_set);
        timeval connect_timeout{};
        connect_timeout.tv_sec = 5;
        connect_timeout.tv_usec = 0;
        const int ready = select(fd + 1, nullptr, &write_set, nullptr, &connect_timeout);
        if (ready <= 0) {
            std::cerr << "connect timed out\n";
            close(fd);
            return std::nullopt;
        }
        int socket_error = 0;
        socklen_t socket_error_size = sizeof(socket_error);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_size) != 0 || socket_error != 0) {
            std::cerr << "connect failed: " << std::strerror(socket_error != 0 ? socket_error : errno) << "\n";
            close(fd);
            return std::nullopt;
        }
    }

    if (fcntl(fd, F_SETFL, flags) != 0) {
        std::cerr << "could not restore blocking socket mode: " << std::strerror(errno) << "\n";
        close(fd);
        return std::nullopt;
    }

    return Socket(fd);
}

bool writeAll(int fd, const std::string& message) {
    const char* cursor = message.data();
    std::size_t remaining = message.size();
    while (remaining > 0) {
        const ssize_t sent = send(fd, cursor, remaining, 0);
        if (sent <= 0) {
            return false;
        }
        cursor += sent;
        remaining -= static_cast<std::size_t>(sent);
    }
    return true;
}

bool writeCommand(int fd, std::string command) {
    if (!command.empty() && command.back() == '\n') command.pop_back();
    if (command.size() <= 4096) return writeAll(fd, command + "\n");
    return writeAll(fd, "FRAME " + std::to_string(command.size()) + "\n") &&
        writeAll(fd, command);
}

std::optional<std::string> readRawLine(int fd) {
    std::string line;
    char ch = '\0';
    while (true) {
        const ssize_t received = recv(fd, &ch, 1, 0);
        if (received == 0) return line.empty() ? std::nullopt : std::optional(line);
        if (received < 0) {
            if (errno == EINTR) continue;
            return std::nullopt;
        }
        if (ch == '\n') return line;
        if (line.size() >= 1024 * 1024) return std::nullopt;
        line.push_back(ch);
    }
}

std::optional<std::string> readMessage(int fd) {
    auto line = readRawLine(fd);
    if (!line.has_value() || line->rfind("FRAME ", 0) != 0) return line;

    std::istringstream in(*line);
    std::string tag, extra;
    std::size_t size = 0;
    in >> tag >> size;
    if (!in || tag != "FRAME" || size == 0 || size > 1024 * 1024 || (in >> extra)) {
        return std::nullopt;
    }
    std::string payload(size, '\0');
    std::size_t offset = 0;
    while (offset < size) {
        const ssize_t received = recv(fd, payload.data() + offset, size - offset, 0);
        if (received < 0 && errno == EINTR) continue;
        if (received <= 0) return std::nullopt;
        offset += static_cast<std::size_t>(received);
    }
    return payload;
}

void printUsage(const char* argv0) {
    std::cerr << "usage: " << argv0 << " [host] [port] [command...]\n"
              << "example:\n"
              << "  " << argv0 << " 127.0.0.1 18889 GET_STATUS\n"
              << "  " << argv0 << " 127.0.0.1 18889 GET_RECORD 500\n";
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 4 || std::string(argv[1]) == "--help") {
        printUsage(argv[0]);
        return 1;
    }

    const std::string host = argv[1];
    const int port = std::stoi(argv[2]);

    std::ostringstream command;
    for (int i = 3; i < argc; ++i) {
        if (i > 3) {
            command << " ";
        }
        command << argv[i];
    }

    auto socket = connectToServer(host, port);
    if (!socket.has_value()) {
        return 1;
    }
    if (!writeCommand(socket->fd(), command.str())) {
        std::cerr << "could not send command\n";
        return 1;
    }
    shutdown(socket->fd(), SHUT_WR);

    while (const auto message = readMessage(socket->fd())) {
        std::cout << *message << "\n";
    }

    return 0;
}
