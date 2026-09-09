#pragma once
// ═══════════════════════════════════════════════════════════════
// platform.h — Cross-platform compatibility layer (Linux / macOS / Windows)
// ═══════════════════════════════════════════════════════════════

#ifdef _WIN32
// ─── Windows headers ───
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <io.h>         // _open, _close, _read, _write, _commit, _chsize, _access, _unlink, _mkdir, _pipe
#include <direct.h>     // _mkdir
#include <fcntl.h>      // O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, O_TRUNC, O_APPEND, _O_BINARY
#include <sys/stat.h>   // _stat64
#include <winsock2.h>
#include <ws2tcpip.h>   // inet_pton, inet_ntop, struct addrinfo
#pragma comment(lib, "ws2_32.lib")

// File open flags
#ifndef O_BINARY
#define O_BINARY _O_BINARY
#endif

// POSIX constants not defined on Windows
#ifndef F_OK
#define F_OK 0
#endif

// ssize_t (POSIX) not available on MSVC
#ifdef _MSC_VER
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif

// ─── Platform function name mapping (CRT) ───
// These macros ensure that ::open(), ::close(), etc. in source files
// transparently map to the Windows CRT underscore-prefixed variants.
#define open    _open
#define close   _close
#define read    _read
#define write   _write
#define unlink  _unlink
#define access  _access
#define fsync   _commit
#define ftruncate _chsize
#define fileno  _fileno
#define stat    _stat64
#define fstat   _fstat64
#define pwrite  platform_pwrite
#define pread   platform_pread

// _rename needs special handling: ::rename() can't be macro-replaced,
// so provide platform_rename() for source files to use
#include <cstdio>
inline int platform_rename(const char* oldname, const char* newname) {
    return rename(oldname, newname);
}

// ─── Network: close() vs closesocket() ───
// On Windows, POSIX close() is #define'd to _close() above.
// For sockets we need closesocket(), so provide a separate macro
// that source files use via socket_close().
// (Not overriding close() to avoid breaking non-socket file I/O.)

// POSIX socket constants not available on Windows
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif
#ifndef SOCK_NONBLOCK
#define SOCK_NONBLOCK 0
#endif

// MS_SYNC not available on Windows
#ifndef MS_SYNC
#define MS_SYNC 0x4
#endif

// Windows max/min macros conflict with std::max/std::min
#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

// M_PI not defined by MSVC unless _USE_MATH_DEFINES is set
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

// Windows setsockopt optval is const char*, cast helper
// (handled per-site in source files)

// Suppress Winsock deprecation warnings (inet_addr etc.)
#ifdef _WIN32
#ifndef _WINSOCK_DEPRECATED_NO_WARNINGS
#define _WINSOCK_DEPRECATED_NO_WARNINGS
#endif
#endif

// Windows mkdir() only takes one argument (no permissions)
// POSIX mkdir(path, mode) is mapped to _mkdir(path) via macro
#ifndef mkdir
#define mkdir(path, mode) _mkdir(path)
#endif

#else
// ─── POSIX (Linux / macOS) ───
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

// Windows-specific constants (no-ops on POSIX)
#ifndef O_BINARY
#define O_BINARY 0
#endif

#endif // _WIN32

// ═══════════════════════════════════════════════════════════════
// File I/O wrappers
// ═══════════════════════════════════════════════════════════════

#ifdef _WIN32

inline ssize_t platform_pread(int fd, void* buf, size_t count, int64_t offset) {
    LARGE_INTEGER li;
    li.QuadPart = offset;
    HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (h == INVALID_HANDLE_VALUE) return -1;
    DWORD bytesRead = 0;
    OVERLAPPED ov = {};
    ov.Offset = li.LowPart;
    ov.OffsetHigh = li.HighPart;
    if (!ReadFile(h, buf, static_cast<DWORD>(count), &bytesRead, &ov))
        return -1;
    return static_cast<ssize_t>(bytesRead);
}

inline ssize_t platform_pwrite(int fd, const void* buf, size_t count, int64_t offset) {
    LARGE_INTEGER li;
    li.QuadPart = offset;
    HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (h == INVALID_HANDLE_VALUE) return -1;
    DWORD bytesWritten = 0;
    OVERLAPPED ov = {};
    ov.Offset = li.LowPart;
    ov.OffsetHigh = li.HighPart;
    if (!WriteFile(h, buf, static_cast<DWORD>(count), &bytesWritten, &ov))
        return -1;
    return static_cast<ssize_t>(bytesWritten);
}

#else

inline ssize_t platform_pread(int fd, void* buf, size_t count, off_t offset) {
    return ::pread(fd, buf, count, offset);
}

inline ssize_t platform_pwrite(int fd, const void* buf, size_t count, off_t offset) {
    return ::pwrite(fd, buf, count, offset);
}

#endif

// ═══════════════════════════════════════════════════════════════
// Memory mapping wrappers
// ═══════════════════════════════════════════════════════════════

#ifdef _WIN32

// Windows protection flags (subset of POSIX PROT_*)
#ifndef PROT_READ
#define PROT_READ  0x1
#endif
#ifndef PROT_WRITE
#define PROT_WRITE 0x2
#endif

// Windows mapping flags
#ifndef MAP_SHARED
#define MAP_SHARED  0x01
#endif
#ifndef MAP_PRIVATE
#define MAP_PRIVATE 0x02
#endif
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS 0x20
#endif

#include <unordered_map>
#include <mutex>

struct PlatformMmapHandle {
    HANDLE file_mapping;
    HANDLE file_handle;
};

static std::unordered_map<void*, PlatformMmapHandle>& get_mmap_registry() {
    static std::unordered_map<void*, PlatformMmapHandle> registry;
    return registry;
}
static std::mutex& get_mmap_mutex() {
    static std::mutex mu;
    return mu;
}

inline void* platform_mmap(void* addr, size_t length, int prot, int flags, int fd, int64_t offset) {
    (void)addr; (void)flags; (void)offset;

    HANDLE hFile = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (hFile == INVALID_HANDLE_VALUE) return nullptr;

    DWORD flProtect = PAGE_READONLY;
    DWORD dwDesiredAccess = FILE_MAP_READ;
    if ((prot & PROT_WRITE) && (flags & MAP_SHARED)) {
        flProtect = PAGE_READWRITE;
        dwDesiredAccess = FILE_MAP_WRITE;
    }

    HANDLE hMapping = CreateFileMappingA(hFile, nullptr, flProtect, 0, 0, nullptr);
    if (hMapping == nullptr) return nullptr;

    void* view = MapViewOfFile(hMapping, dwDesiredAccess, 0, 0, length);
    if (view == nullptr) {
        CloseHandle(hMapping);
        return nullptr;
    }

    // Store mapping handle in a global registry (no pointer offset)
    {
        std::lock_guard<std::mutex> lock(get_mmap_mutex());
        get_mmap_registry()[view] = {hMapping, hFile};
    }
    return view;
}

inline int platform_munmap(void* addr, size_t length) {
    (void)length;
    if (!addr) return 0;
    std::lock_guard<std::mutex> lock(get_mmap_mutex());
    auto& registry = get_mmap_registry();
    auto it = registry.find(addr);
    if (it != registry.end()) {
        UnmapViewOfFile(addr);
        CloseHandle(it->second.file_mapping);
        registry.erase(it);
    }
    return 0;
}

inline int platform_msync(void* addr, size_t length, int /* flags */) {
    if (!addr) return 0;
    FlushViewOfFile(addr, length);
    return 0;
}

#else

inline void* platform_mmap(void* addr, size_t length, int prot, int flags, int fd, off_t offset) {
    return ::mmap(addr, length, prot, flags, fd, offset);
}

inline int platform_munmap(void* addr, size_t length) {
    return ::munmap(addr, length);
}

inline int platform_msync(void* addr, size_t length, int flags) {
    return ::msync(addr, length, flags);
}

#endif

#ifndef MAP_FAILED
#define MAP_FAILED ((void*)-1)
#endif

// ═══════════════════════════════════════════════════════════════
// Disk space
// ═══════════════════════════════════════════════════════════════

#ifdef _WIN32

struct platform_statvfs_t {
    uint64_t f_bavail;  // available blocks
    uint64_t f_frsize;  // fragment size (block size)
};

inline int platform_statvfs(const char* path, platform_statvfs_t* buf) {
    ULARGE_INTEGER freeBytesAvailable;
    if (!GetDiskFreeSpaceExA(path, &freeBytesAvailable, nullptr, nullptr))
        return -1;
    buf->f_frsize = 1;
    buf->f_bavail = freeBytesAvailable.QuadPart;
    return 0;
}

#else

inline int platform_statvfs(const char* path, struct statvfs* buf) {
    return ::statvfs(path, buf);
}

#endif

// ═══════════════════════════════════════════════════════════════
// Networking (Winsock2 on Windows, BSD sockets on POSIX)
// ═══════════════════════════════════════════════════════════════

#ifdef _WIN32

typedef SOCKET socket_t;
#define SOCKET_INVALID INVALID_SOCKET
#define SOCKET_ERROR_VALUE SOCKET_ERROR

inline int socket_close(socket_t s) { return closesocket(s); }

inline int socket_set_nonblocking(socket_t s) {
    u_long mode = 1;
    return ioctlsocket(s, FIONBIO, &mode);
}

inline int socket_set_blocking(socket_t s) {
    u_long mode = 0;
    return ioctlsocket(s, FIONBIO, &mode);
}

#else

typedef int socket_t;
#define SOCKET_INVALID (-1)
#define SOCKET_ERROR_VALUE (-1)

inline int socket_close(int fd) { return ::close(fd); }

inline int socket_set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

inline int socket_set_blocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    return ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}

#endif

// ═══════════════════════════════════════════════════════════════
// Event notification (pipe-based, portable)
// ═══════════════════════════════════════════════════════════════

#ifdef _WIN32

inline int platform_pipe(int pipefd[2]) {
    return _pipe(pipefd, 4096, _O_BINARY);
}

#else

inline int platform_pipe(int pipefd[2]) {
    return ::pipe(pipefd);
}

#endif

// ═══════════════════════════════════════════════════════════════
// Platform init / cleanup helpers
// ═══════════════════════════════════════════════════════════════

#ifdef _WIN32

struct WinsockInit {
    WinsockInit() {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
    }
    ~WinsockInit() { WSACleanup(); }
};

#endif
