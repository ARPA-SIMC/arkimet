#include "session.h"
#include "arki/segment/iseg.h"
#include "index.h"

namespace arki::segment::iseg {

std::shared_ptr<arki::Segment> Session::segment_from_relpath_and_format(const std::filesystem::path& relpath, DataFormat format) const
{
    return std::make_shared<arki::segment::iseg::Segment>(shared_from_this(), format, relpath);
}

std::shared_ptr<RIndex> Session::read_index(const std::filesystem::path& data_relpath, std::shared_ptr<core::ReadLock> lock)
{
    return std::make_shared<RIndex>(std::static_pointer_cast<iseg::Session>(shared_from_this()), data_relpath, lock);
}

std::shared_ptr<AIndex> Session::append_index(std::shared_ptr<segment::data::Writer> segment, std::shared_ptr<core::AppendLock> lock)
{
    return std::make_shared<AIndex>(std::static_pointer_cast<iseg::Session>(shared_from_this()), segment, lock);
}

std::shared_ptr<CIndex> Session::check_index(const std::filesystem::path& data_relpath, std::shared_ptr<core::CheckLock> lock)
{
    return std::make_shared<CIndex>(std::static_pointer_cast<iseg::Session>(shared_from_this()), data_relpath, lock);
}

}
