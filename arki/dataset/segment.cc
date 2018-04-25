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

std::shared_ptr<segment::Reader> SegmentManager::get_reader(const std::string& relpath, std::shared_ptr<core::Lock> lock)
{
    return get_reader(utils::get_format(relpath), relpath, lock);
}

std::shared_ptr<segment::Reader> SegmentManager::get_reader(const std::string& format, const std::string& relpath, std::shared_ptr<core::Lock> lock)
{
    return segment::Reader::for_pathname(format, root, relpath, str::joinpath(root, relpath), lock);
}

std::shared_ptr<segment::Writer> SegmentManager::get_writer(const std::string& relpath)
{
    return get_writer(utils::get_format(relpath), relpath);
}

std::shared_ptr<segment::Writer> SegmentManager::get_writer(const std::string& format, const std::string& relpath)
{
    // Ensure that the directory containing the segment exists
    string abspath = str::joinpath(root, relpath);
    sys::makedirs(str::dirname(abspath));

    return create_writer_for_format(format, relpath, abspath);
}

std::shared_ptr<segment::Checker> SegmentManager::get_checker(const std::string& relpath)
{
    return get_checker(utils::get_format(relpath), relpath);
}

std::shared_ptr<segment::Checker> SegmentManager::get_checker(const std::string& format, const std::string& relpath)
{
    string abspath = str::joinpath(root, relpath);
    return create_checker_for_format(format, relpath, abspath);
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
