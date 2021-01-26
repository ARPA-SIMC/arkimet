#include "exceptions.h"
#include <system_error>
#include <cerrno>
#include <stdexcept>

namespace arki {

void throw_system_error(const std::string& what)
{
    throw std::system_error(errno, std::system_category(), what);
}

void throw_file_error(const std::string& file, const std::string& what)
{
    throw std::system_error(errno, std::system_category(), file + ": " + what);
}

void throw_consistency_error(const std::string& context, const std::string& error)
{
    throw std::runtime_error(error + " (" + context + ")");
}

void throw_consistency_error(const std::string& error)
{
    throw std::runtime_error(error);
}

}
