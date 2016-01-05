#include "exceptions.h"
#include <system_error>
#include <cerrno>

namespace arki {

void throw_system_error(const std::string& what)
{
    throw std::system_error(errno, std::system_category(), what);
}

}
