#ifndef WIBBLE_SYS_DIRECTORY_H
#define WIBBLE_SYS_DIRECTORY_H

#include <string>
#include <unistd.h>     // access

struct dirent;

namespace wibble {
namespace sys {
namespace fs {

/// access() a filename
bool access(const std::string& s, int m);

/// Same as access(s, F_OK);
bool exists(const std::string& s);

}
}
}

// vim:set ts=4 sw=4:
#endif
