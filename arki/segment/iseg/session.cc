#include "session.h"
#include "index.h"
#include "arki/segment/iseg.h"
#include "arki/segment/data.h"
#include "arki/nag.h"

namespace arki::segment::iseg {

std::shared_ptr<arki::Segment> Session::segment_from_relpath_and_format(const std::filesystem::path& relpath, DataFormat format) const
{
    return std::make_shared<arki::segment::iseg::Segment>(shared_from_this(), format, relpath);
}


std::shared_ptr<segment::Reader> Session::segment_reader(std::shared_ptr<const arki::Segment> segment, std::shared_ptr<const core::ReadLock> lock) const
{
    auto data = segment->data();
    if (!data->timestamp())
    {
        nag::warning("%s: segment data is not available", segment->abspath().c_str());
        return std::make_shared<segment::EmptyReader>(segment, lock);
    }
    return std::make_shared<Reader>(std::static_pointer_cast<const iseg::Segment>(segment), lock);
}

std::shared_ptr<segment::Checker> Session::segment_checker(std::shared_ptr<const arki::Segment> segment, std::shared_ptr<core::CheckLock> lock) const
{
    return std::make_shared<Checker>(std::static_pointer_cast<const iseg::Segment>(segment), lock);
}


std::shared_ptr<AIndex> Session::append_index(std::shared_ptr<const arki::Segment> segment, std::shared_ptr<core::AppendLock> lock) const
{
    return std::make_shared<AIndex>(std::static_pointer_cast<const iseg::Segment>(segment), lock);
}

std::shared_ptr<CIndex> Session::check_index(std::shared_ptr<const arki::Segment> segment, std::shared_ptr<core::CheckLock> lock) const
{
    return std::make_shared<CIndex>(std::static_pointer_cast<const iseg::Segment>(segment), lock);
}


void Session::create_iseg(std::shared_ptr<arki::Segment> segment, arki::metadata::Collection& mds) const
{
    auto data = segment->data();
    data->create_segment(mds);
    // TODO: implement data->read_lock() and data->check_lock()
    auto lock = std::make_shared<core::lock::NullCheckLock>();
    auto checker = std::make_shared<arki::segment::iseg::Checker>(std::dynamic_pointer_cast<iseg::Segment>(segment), lock);
    auto fixer = checker->fixer();
    fixer->reindex(mds);
}

}
