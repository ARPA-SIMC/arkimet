#include <arki/wibble/sys/fs.h>
#include <sys/stat.h>
#include <unistd.h>

namespace wibble {
namespace sys {
namespace fs {

bool access(const std::string &s, int m)
{
	return ::access(s.c_str(), m) == 0;
}

bool exists(const std::string& file)
{
    return sys::fs::access(file, F_OK);
}


}
}
}
