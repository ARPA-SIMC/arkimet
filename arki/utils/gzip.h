#ifndef ARKI_UTILS_GZIP_H
#define ARKI_UTILS_GZIP_H

/// zlib wrappers

#include <arki/libconfig.h>
#include <string>
#include <zlib.h>
#include <vector>

namespace arki {
namespace utils {
namespace gzip {

class File
{
protected:
    gzFile fd = nullptr;
    std::string pathname;

    /// Throw an exception adding the error message from gzerror
    [[noreturn]] void throw_error(const char* desc);

    /// Throw an exception unrelated from gzerror
    [[noreturn]] void throw_runtime_error(const char* desc);

public:
    /// Instantiate a File but do not open it yet
    File(const std::string& pathname);

    /// Wrapper around gzdopen
    File(const std::string& pathname, int fd, const char* mode);

    /// Wrapper around gzopen
    File(const std::string& pathname, const char* mode);

    /// Calls gzclose if the file is still open, but it will ignore its result
    ~File();

    /**
     * Wrapper around gzclose, with error checking.
     *
     * Sets fd to nullptr, and checks if fd is nullptr, so it can be called
     * multiple times.
     */
    void close();

    /**
     * Wrapper around gzfdopen.
     *
     * If the file is currently open, it will be closed first.
     */
    void fdopen(int fd, const char* mode);

    /// Wrapper around gzread
    unsigned read(void* buf, unsigned len);

    /// Call read and throw runtime_error if the read was incomplete
    void read_all_or_throw(void* buf, unsigned len);

    /// Read until the end of the file
    std::vector<uint8_t> read_all();

    /// Wrapper around gzseek
    z_off_t seek(z_off_t offset, int whence);

    /// Return the file pathname
    const std::string& name() const { return pathname; }

    operator gzFile() const { return fd; }
};

}
}
}
#endif

