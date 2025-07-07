#include "defs.h"
#include "arki/utils/string.h"
#include <string>
#include <vector>

using namespace arki::utils;

namespace arki::segment {

std::string State::to_string() const
{
    std::vector<const char*> res;
    if (is_ok())
        res.push_back("OK");
    if (has_any(SEGMENT_DIRTY))
        res.push_back("DIRTY");
    if (has_any(SEGMENT_UNALIGNED))
        res.push_back("UNALIGNED");
    if (has_any(SEGMENT_MISSING))
        res.push_back("MISSING");
    if (has_any(SEGMENT_DELETED))
        res.push_back("DELETED");
    if (has_any(SEGMENT_CORRUPTED))
        res.push_back("CORRUPTED");
    if (has_any(SEGMENT_ARCHIVE_AGE))
        res.push_back("ARCHIVE_AGE");
    if (has_any(SEGMENT_DELETE_AGE))
        res.push_back("DELETE_AGE");
    // if (has_any(SEGMENT_UNOPTIMIZED)) res.push_back("UNOPTIMIZED");
    return str::join(",", res.begin(), res.end());
}

std::ostream& operator<<(std::ostream& o, const State& s)
{
    return o << s.to_string();
}

} // namespace arki::segment
