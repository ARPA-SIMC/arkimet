#include "arki/dataset/simple/datafile.h"
#include "arki/metadata.h"
#include "arki/core/file.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/nag.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

namespace datafile {

MdBuf::MdBuf(const std::string& pathname, std::shared_ptr<core::Lock> lock)
    : dir(str::dirname(pathname)), basename(str::basename(pathname)), pathname(pathname), flushed(true)
{
    struct stat st_data;
    if (!dir.fstatat_ifexists(basename.c_str(), st_data))
        return;

    // Read the metadata
    scan::scan(pathname, lock, mds.inserter_func());

    // Read the summary
    if (!mds.empty())
        mds.add_to_summary(sum);
}

MdBuf::~MdBuf()
{
    flush();
}

void MdBuf::add(const Metadata& md, const types::source::Blob& source)
{
    using namespace arki::types;

    // Replace the pathname with its basename
    unique_ptr<Metadata> copy(md.clone());
    copy->set_source(Source::createBlobUnlocked(source.format, dir.name(), basename, source.offset, source.size));
    sum.add(*copy);
    mds.acquire(move(copy));
    flushed = false;
}

void MdBuf::flush()
{
    if (flushed) return;
    mds.writeAtomically(pathname + ".metadata");
    sum.writeAtomically(pathname + ".summary");
    //fsync(dir);
}

}

}
}
}
