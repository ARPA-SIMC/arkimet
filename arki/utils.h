#ifndef ARKI_UTILS_H
#define ARKI_UTILS_H

#include <sstream>
#include <string>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <cstdint>

namespace arki {
namespace utils {

/// Create an empty file, succeeding if it already exists
void createFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewFlagfile(const std::string& pathname);

#if 0
template<typename A, typename B>
int compareMaps(const A& m1, const B& m2)
{
        typename A::const_iterator a = m1.begin();
        typename B::const_iterator b = m2.begin();
        for ( ; a != m1.end() && b != m2.end(); ++a, ++b)
        {
                if (a->first < b->first)
                        return -1;
                if (a->first > b->first)
                        return 1;
                if (int res = a->second->compare(*b->second)) return res;
        }
        if (a == m1.end() && b == m2.end())
                return 0;
        if (a == m1.end())
                return -1;
        return 1;
}
#endif

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

/**
 * RAII-style class changing into a newly created temporary directory during
 * the lifetime of the object.
 *
 * The temporary directory is created at constructor time and deleted at
 * destructor time.
 */
struct MoveToTempDir
{
    std::string old_dir;
    std::string tmp_dir;

    MoveToTempDir(const std::string& pattern = "/tmp/tmpdir.XXXXXX");
    ~MoveToTempDir();
};

/**
 * Get format from file extension
 *
 * @returns the empty string if no file extension was found
 */
std::string get_format(const std::string& fname);

/**
 * Get format from file extension, throwing an exception if no extension was
 * found
 */
std::string require_format(const std::string& fname);

}
}

#endif
