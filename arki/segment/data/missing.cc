#include "missing.h"
#include "arki/types/source/blob.h"
#include <sstream>

using namespace std;
using namespace std::string_literals;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki::segment::data::missing {

bool Reader::scan_data(metadata_dest_func dest)
{
    throw std::runtime_error("cannot scan "s + segment().abspath.native() + ": segment has disappeared");
}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    stringstream ss;
    ss << "cannot read " << src.size << " bytes of " << src.format << " data from " << segment().abspath << ":"
       << src.offset << ": the segment has disappeared";
    throw std::runtime_error(ss.str());
}

stream::SendResult Reader::stream(const types::source::Blob& src, StreamOutput& out)
{
    stringstream ss;
    ss << "cannot stream " << src.size << " bytes of " << src.format << " data from " << segment().abspath << ":"
       << src.offset << ": the segment has disappeared";
    throw std::runtime_error(ss.str());
}

}
