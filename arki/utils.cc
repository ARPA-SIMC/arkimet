#include "arki/utils.h"
#include "arki/exceptions.h"
#include "arki/utils/sys.h"
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

using namespace std;

namespace arki {
namespace utils {

size_t parse_size(const std::string& str)
{
    const char* s = str.c_str();
    char* suffix;
    unsigned long long int res = strtoull(s, &suffix, 10);
    if (!*suffix) return res;

    int mul;
    if (!*(suffix + 1))
        mul = 1000;
    else
    {
        if (*(suffix + 2))
            throw std::runtime_error(str + ": unknown suffix: " + suffix);

        switch (tolower(*(suffix + 1)))
        {
            case 'b':
                if (*suffix == 'b' || *suffix == 'c')
                    throw std::runtime_error(str + ": unknown suffix: " + suffix);
                mul = 1000;
                break;
            case 'i': mul = 1024; break;
            default: throw std::runtime_error(str + ": unknown suffix: " + suffix);
        }
    }

    switch (tolower(*suffix))
    {
        case 'c': return res;
        case 'b': return res;
        case 'k': return res * mul;
        case 'm': return res * mul * mul;
        case 'g': return res * mul * mul * mul;
        case 't': return res * mul * mul * mul * mul;
        case 'p': return res * mul * mul * mul * mul * mul;
        case 'e': return res * mul * mul * mul * mul * mul * mul;
        case 'z': return res * mul * mul * mul * mul * mul * mul * mul;
        case 'y': return res * mul * mul * mul * mul * mul * mul * mul * mul;
        default: throw std::runtime_error(str + ": unknown suffix: " + suffix);
    }
}

}
}
