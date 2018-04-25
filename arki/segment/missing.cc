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


const char* Segment::type() const { return "missing"; }
bool Segment::single_file() const { return true; }


Reader::Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
   : segment::Reader(lock), m_segment(format, root, relpath, abspath)
{
}

const Segment& Reader::segment() const { return m_segment; }

time_t Reader::timestamp()
{
    throw std::runtime_error("cannot get mtime of " + m_segment.abspath + ": segment has disappeared");
}

bool Reader::scan_data(metadata_dest_func dest)
{
    throw std::runtime_error("cannot scan " + m_segment.abspath + ": segment has disappeared");
}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    stringstream ss;
    ss << "cannot read " << src.size << " bytes of " << src.format << " data from " << m_segment.abspath << ":"
       << src.offset << ": the segment has disappeared";
    throw std::runtime_error(ss.str());
}

size_t Reader::stream(const types::source::Blob& src, core::NamedFileDescriptor& out)
{
    stringstream ss;
    ss << "cannot stream " << src.size << " bytes of " << src.format << " data from " << m_segment.abspath << ":"
       << src.offset << ": the segment has disappeared";
    throw std::runtime_error(ss.str());
}

}
}
}
