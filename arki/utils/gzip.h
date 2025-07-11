#ifndef ARKI_UTILS_GZIP_H
#define ARKI_UTILS_GZIP_H

/// zlib wrappers

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>
#include <zlib.h>

namespace arki {
namespace utils {
namespace gzip {

class File
{
protected:
    gzFile fd = nullptr;
    std::filesystem::path pathname;

    /// Throw an exception adding the error message from gzerror
    [[noreturn]] void throw_error(const char* desc);

    /// Throw an exception unrelated from gzerror
    [[noreturn]] void throw_runtime_error(const char* desc);

public:
    /// Instantiate a File but do not open it yet
    File(const std::filesystem::path& pathname);

    /// Wrapper around gzdopen
    File(const std::filesystem::path& pathname, int fd, const char* mode);

    /// Wrapper around gzopen
    File(const std::filesystem::path& pathname, const char* mode);

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
    [[deprecated("Use path() instead")]] const std::string& name() const
    {
        return pathname.native();
    }
    const std::filesystem::path& path() const { return pathname; }

    operator gzFile() const { return fd; }
};

} // namespace gzip
} // namespace utils
} // namespace arki
#endif
