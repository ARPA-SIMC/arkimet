#include "missing.h"
#include "arki/exceptions.h"
#include "arki/types/source/blob.h"
#include <sstream>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace segment {
namespace missing {

Reader::Reader(const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
   : segment::Reader(root, relpath, abspath, lock)
{
}

const char* Reader::type() const { return "missing"; }
bool Reader::single_file() const { return true; }

bool Reader::scan(metadata_dest_func dest)
{
    throw std::runtime_error("cannot scan " + abspath + ": segment has disappeared");
}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    stringstream ss;
    ss << "cannot read " << src.size << " bytes of " << src.format << " data from " << abspath << ":"
       << src.offset << ": the segment has disappeared";
    throw std::runtime_error(ss.str());
}

size_t Reader::stream(const types::source::Blob& src, core::NamedFileDescriptor& out)
{
    stringstream ss;
    ss << "cannot stream " << src.size << " bytes of " << src.format << " data from " << abspath << ":"
       << src.offset << ": the segment has disappeared";
    throw std::runtime_error(ss.str());
}

}
}
}
