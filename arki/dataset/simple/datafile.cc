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
    : pathname(pathname), dirname(str::dirname(pathname)), basename(str::basename(pathname)), flushed(true)
{
    if (sys::exists(pathname))
    {
        // Read the metadata
        scan::scan(pathname, mds.inserter_func());

        // Read the summary
        if (!mds.empty())
        {
            string sumfname = pathname + ".summary";
            if (sys::timestamp(pathname, 0) <= sys::timestamp(sumfname, 0))
                sum.readFile(sumfname);
            else
                mds.add_to_summary(sum);
        }
    }
}

MdBuf::~MdBuf()
{
    flush();
}

void MdBuf::add(const Metadata& md)
{
    using namespace arki::types;

    const source::Blob& os = md.sourceBlob();

    // Replace the pathname with its basename
    unique_ptr<Metadata> copy(md.clone());
    copy->set_source(Source::createBlob(os.format, dirname, basename, os.offset, os.size));
    sum.add(*copy);
    mds.eat(move(copy));
    flushed = false;
}

void MdBuf::flush()
{
    if (flushed) return;
    mds.writeAtomically(pathname + ".metadata");
    sum.writeAtomically(pathname + ".summary");
}

}

}
}
}
