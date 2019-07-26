#ifndef ARKI_UTILS_IOSTREAM_H
#define ARKI_UTILS_IOSTREAM_H

#include <iostream>

namespace arki {
namespace utils {

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

}
}

#endif
