#ifndef ARKI_EXCEPTIONS_H
#define ARKI_EXCEPTIONS_H

#include <filesystem>
#include <string>
#include <sstream>

namespace arki {

/**
 * Shortcut to throw std::system_error using the current value of errno
 */
[[noreturn]] void throw_system_error(const std::string& what);

/**
 * Shortcut to throw std::system_error using the current value of errno and
 * mentioning the name of the file for which the operation failed.
 */
[[noreturn]] void throw_file_error(const std::filesystem::path& file, const std::string& what);

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

template<typename... Args>
[[noreturn]] void throw_system_error(int errno_value, Args... args)
{
    std::stringstream buf;
    (buf << ... << args);
    throw std::system_error(errno_value, std::system_category(), buf.str());
}

template<typename... Args>
[[noreturn]] void throw_runtime_error(Args... args)
{
    std::stringstream buf;
    (buf << ... << args);
    throw std::runtime_error(buf.str());
}

}

#endif
