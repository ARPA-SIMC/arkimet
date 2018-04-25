#ifndef ARKI_UTILS_H
#define ARKI_UTILS_H

#include <sstream>
#include <string>
#include <cstdint>

namespace arki {
namespace utils {

/// Create an empty file, succeeding if it already exists
void createFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewFlagfile(const std::string& pathname);

// Save the state of a stream, the RAII way
class SaveIOState
{
		std::ios_base& s;
		std::ios_base::fmtflags f;
public:
		SaveIOState(std::ios_base& s) : s(s), f(s.flags())
		{}
		~SaveIOState()
		{
				s.flags(f);
		}
};

// Dump the string, in hex, to stderr, prefixed with name
void hexdump(const char* name, const std::string& str);

// Dump the string, in hex, to stderr, prefixed with name
void hexdump(const char* name, const unsigned char* str, int len);

}
}

#endif
