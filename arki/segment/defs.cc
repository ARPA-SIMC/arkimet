#include "defs.h"
#include "arki/utils/string.h"
#include <string>
#include <vector>

using namespace arki::utils;

namespace arki::segment {

std::string State::to_string() const
{
    std::vector<const char*> res;
    if (value == SEGMENT_OK.value)         res.push_back("OK");
    if (value & SEGMENT_DIRTY.value)       res.push_back("DIRTY");
    if (value & SEGMENT_UNALIGNED.value)   res.push_back("UNALIGNED");
    if (value & SEGMENT_MISSING.value)     res.push_back("MISSING");
    if (value & SEGMENT_DELETED.value)     res.push_back("DELETED");
    if (value & SEGMENT_CORRUPTED.value)   res.push_back("CORRUPTED");
    if (value & SEGMENT_ARCHIVE_AGE.value) res.push_back("ARCHIVE_AGE");
    if (value & SEGMENT_DELETE_AGE.value)  res.push_back("DELETE_AGE");
    return str::join(",", res.begin(), res.end());
}

std::ostream& operator<<(std::ostream& o, const State& s)
{
    return o << s.to_string();
}

}
