#ifndef ARKI_EXCEPTIONS_H
#define ARKI_EXCEPTIONS_H

#include <string>

namespace arki {

/**
 * Shortcut to throw std::system_error using the current value of errno
 */
[[noreturn]] void throw_system_error(const std::string& what);

/**
 * Shortcut to throw std::system_error using the current value of errno and
 * mentioning the name of the file for which the operation failed.
 */
[[noreturn]] void throw_file_error(const std::string& file, const std::string& what);

}

#endif
