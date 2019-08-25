#ifndef ARKI_UTILS_H
#define ARKI_UTILS_H

#include <string>
#include <cstdint>

namespace arki {
namespace utils {

/// Parse a string in the form <number><suffix> returning its value in bytes
size_t parse_size(const std::string& str);

}
}

#endif
