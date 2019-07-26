#ifndef ARKI_UTILS_H
#define ARKI_UTILS_H

#include <string>
#include <cstdint>

namespace arki {
namespace utils {

/// Create an empty file, succeeding if it already exists
void createFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewFlagfile(const std::string& pathname);

// Dump the string, in hex, to stderr, prefixed with name
void hexdump(const char* name, const std::string& str);

// Dump the string, in hex, to stderr, prefixed with name
void hexdump(const char* name, const unsigned char* str, int len);

/// Parse a string in the form <number><suffix> returning its value in bytes
size_t parse_size(const std::string& str);

}
}

#endif
