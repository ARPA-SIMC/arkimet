#include "config.h"
#include <arki/utils/fd.h>
#include <arki/wibble/exception.h>
#include <sys/stat.h>
#include <unistd.h>


using namespace std;

namespace arki {
namespace utils {
namespace fd {

void HandleWatch::close()
{
    if (fd > 0)
    {
        if (::close(fd) < 0)
            throw wibble::exception::File(fname, "closing file");
        fd = -1;
    }
}

TempfileHandleWatch::~TempfileHandleWatch()
{
    if (fd > 0)
    {
        close();
        ::unlink(fname.c_str());
    }
}

void write_all(int fd, const void* buf, size_t len)
{
    size_t done = 0;
    while (done < len)
    {
        ssize_t res = write(fd, (const unsigned char*)buf+done, len-done);
        if (res < 0)
            throw wibble::exception::System("writing to file descriptor");
        done += res;
    }
}

}
}
}
