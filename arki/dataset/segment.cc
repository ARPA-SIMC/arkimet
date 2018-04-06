#include "segment.h"
#include "segment/managers.h"
#include "arki/segment/concat.h"
#include "arki/segment/lines.h"
#include "arki/segment/dir.h"
#include "arki/segment/tar.h"
#include "arki/configfile.h"
#include "arki/exceptions.h"
#include "arki/scan/any.h"
#include "arki/metadata/collection.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

SegmentManager::SegmentManager(const std::string& root)
    : root(root)
{
}

SegmentManager::~SegmentManager()
{
}

std::shared_ptr<segment::Writer> SegmentManager::get_writer(const std::string& relname)
{
    return get_writer(utils::get_format(relname), relname);
}

std::shared_ptr<segment::Writer> SegmentManager::get_writer(const std::string& format, const std::string& relname)
{
    // Ensure that the directory for 'relname' exists
    string absname = str::joinpath(root, relname);
    size_t pos = absname.rfind('/');
    if (pos != string::npos)
        sys::makedirs(absname.substr(0, pos));

    // Refuse to write to compressed files
    if (scan::isCompressed(absname))
        throw_consistency_error("accessing data file " + relname,
                "cannot update compressed data files: please manually uncompress it first");

    // Else we need to create an appropriate one
    return create_writer_for_format(format, relname, absname);
}

std::shared_ptr<segment::Checker> SegmentManager::get_checker(const std::string& relname)
{
    return get_checker(utils::get_format(relname), relname);
}

std::shared_ptr<segment::Checker> SegmentManager::get_checker(const std::string& format, const std::string& relname)
{
    string absname = str::joinpath(root, relname);
    return create_checker_for_format(format, relname, absname);
}

std::unique_ptr<SegmentManager> SegmentManager::get(const std::string& root, bool force_dir, bool mock_data)
{
    if (force_dir)
        if (mock_data)
            return unique_ptr<SegmentManager>(new HoleDirManager(root));
        else
            return unique_ptr<SegmentManager>(new ForceDirManager(root));
    else
        return unique_ptr<SegmentManager>(new AutoManager(root, mock_data));
}

}
}
