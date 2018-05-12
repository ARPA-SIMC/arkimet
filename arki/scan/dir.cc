#include "dir.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/segment.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include <algorithm>

using namespace std;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace scan {

Dir::Dir() {}

Dir::~Dir()
{
}

std::unique_ptr<Metadata> Dir::scan_data(const std::vector<uint8_t>& data)
{
    throw std::runtime_error("scan_data not available on dir scanner");
}

size_t Dir::scan_singleton(const std::string& abspath, Metadata& md)
{
    throw std::runtime_error("scan_singleton not available on dir scanner");
}

bool Dir::scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest)
{
    throw std::runtime_error("scan_pipe not available on dir scanner");
}

bool Dir::scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    // Collect all file names in the directory
    const auto& segment = reader->segment();
    std::vector<std::pair<size_t, std::string>> fnames;
    sys::Path dir(segment.abspath);
    for (sys::Path::iterator di = dir.begin(); di != dir.end(); ++di)
        if (di.isreg() && str::endswith(di->d_name, segment.format))
        {
            char* endptr;
            size_t pos = strtoull(di->d_name, &endptr, 10);
            if (endptr == di->d_name) continue;
            if (*endptr != '.') continue;
            fnames.emplace_back(make_pair(pos, di->d_name));
        }

    // Sort them numerically
    std::sort(fnames.begin(), fnames.end());

    // Scan them one by one
    auto scanner = scan::Scanner::get_scanner(segment.format);
    for (const auto& fi : fnames)
    {
        unique_ptr<Metadata> md(new Metadata);
        size_t size = scanner->scan_singleton(str::joinpath(segment.abspath, fi.second), *md);
        if (!size)
            continue;
        md->set_source(Source::createBlob(reader, fi.first, size));
        if (!dest(std::move(md)))
            return false;
    }

    return true;
}

}
}
