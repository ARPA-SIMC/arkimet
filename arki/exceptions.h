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

/**
 * Wrapper used to easily replace old wibble::exception::Consistency, to be
 * deprecated in the future in favour of using std::runtime_error directly.
 */
[[noreturn]] void throw_consistency_error(const std::string& context, const std::string& error);

/**
 * Wrapper used to easily replace old wibble::exception::Consistency, to be
 * deprecated in the future in favour of using std::runtime_error directly.
 */
[[noreturn]] void throw_consistency_error(const std::string& error);

}

#endif
