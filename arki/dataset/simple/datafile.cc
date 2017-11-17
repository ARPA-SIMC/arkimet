#include "config.h"
#include "arki/dataset/simple/datafile.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

namespace datafile {

MdBuf::MdBuf(const std::string& pathname)
    : dir(str::dirname(pathname)), basename(str::basename(pathname)), pathname(pathname), flushed(true)
{
    struct stat st_data;
    if (!dir.fstatat_ifexists(basename.c_str(), st_data))
        return;

    // Read the metadata
    scan::scan(pathname, mds.inserter_func());

    // Read the summary
    if (!mds.empty())
    {
        //struct stat st_summary;
        //if (dir.fstatat_ifexists((basename + ".summary").c_str(), st_summary) && st_summary.st_mtime >= st_data.st_mtime)
        //    sum.readFile(pathname + ".summary");
        //else
            mds.add_to_summary(sum);
    }
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
