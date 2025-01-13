#include "session.h"
#include "arki/segment/index/iseg.h"

namespace arki::segment::index::iseg {

std::shared_ptr<RIndex> Session::read_index(const std::filesystem::path& data_relpath, std::shared_ptr<core::ReadLock> lock)
{
    return std::make_shared<RIndex>(std::static_pointer_cast<Session>(shared_from_this()), data_relpath, lock);
}

std::shared_ptr<AIndex> Session::append_index(std::shared_ptr<segment::data::Writer> segment, std::shared_ptr<core::AppendLock> lock)
{
    return std::make_shared<AIndex>(std::static_pointer_cast<Session>(shared_from_this()), segment, lock);
}

std::shared_ptr<CIndex> Session::check_index(const std::filesystem::path& data_relpath, std::shared_ptr<core::CheckLock> lock)
{
    return std::make_shared<CIndex>(std::static_pointer_cast<Session>(shared_from_this()), data_relpath, lock);
}

}
