#include "gzip.h"
#include <system_error>
#include <cerrno>

using namespace std;

namespace arki {
namespace utils {
namespace gzip {

File::File(const std::string& pathname)
    : pathname(pathname)
{
}

File::File(const std::string& pathname, int fd, const char* mode)
    : pathname(pathname)
{
    fdopen(fd, mode);
}

File::File(const std::string& pathname, const char* mode)
    : pathname(pathname)
{
    // Open the new file
    fd = gzopen(pathname.c_str(), mode);
    if (fd == nullptr)
    {
        string msg = pathname;
        msg += ": cannot open file";
        throw std::system_error(errno, std::system_category(), std::move(msg));
    }
}

File::~File()
{
    if (fd != nullptr) gzclose(fd);
}

void File::throw_error(const char* desc)
{
    int errnum;
    const char *gzmsg = gzerror(fd, &errnum);
    if (errnum == Z_ERRNO)
    {
        string msg = pathname;
        msg += ": ";
        msg += desc;
        throw std::system_error(errno, std::system_category(), std::move(msg));
    }
    else
    {
        string msg = pathname;
        msg += ": ";
        msg += desc;
        msg += ": ";
        msg += gzmsg;
        throw std::runtime_error(std::move(msg));
    }
}

void File::throw_runtime_error(const char* desc)
{
    string msg = pathname;
    msg += ": ";
    msg += desc;
    throw std::runtime_error(std::move(msg));
}

void File::close()
{
    if (fd == nullptr) return;

    int res = gzclose(fd);
    fd = nullptr;
    if (res != Z_OK) throw_error("cannot close");
}

void File::fdopen(int fd, const char* mode)
{
    close();

    this->fd = gzdopen(fd, mode);
    if (this->fd == nullptr)
    {
        string msg = pathname;
        msg += ": gzdopen failed";
        throw std::system_error(errno, std::system_category(), std::move(msg));
    }
}

z_off_t File::seek(z_off_t offset, int whence)
{
    z_off_t res = gzseek(fd, offset, whence);
    if (res == -1)
        throw_error("cannot seek");
    return res;
}

unsigned File::read(void* buf, unsigned len)
{
    int res = gzread(fd, buf, len);
    if (res == -1)
        throw_error("cannot read");
    return res;
}

void File::read_all_or_throw(void* buf, unsigned len)
{
    unsigned res = read(buf, len);
    if (res != len)
        throw_runtime_error("incomplete read");
}

}
}
}
