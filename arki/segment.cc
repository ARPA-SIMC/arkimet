#include "segment.h"

using namespace std;
using namespace std::string_literals;

namespace arki {

Segment::Segment(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
    : format(format), root(root), relpath(relpath), abspath(abspath)
{
}

Segment::~Segment()
{
}

}
