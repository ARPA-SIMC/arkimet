#include "simple.h"
#include "arki/dataset/lock.h"
#include "arki/nag.h"
#include "arki/segment/data.h"
#include "arki/segment/metadata.h"
#include "simple/checker.h"
#include "simple/reader.h"
#include "simple/writer.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

std::shared_ptr<segment::Reader> SegmentSession::create_segment_reader(
    std::shared_ptr<const Segment> segment,
    std::shared_ptr<const core::ReadLock> lock) const
{
    auto md_abspath = sys::with_suffix(segment->abspath(), ".metadata");
    if (auto st_md = sys::stat(md_abspath))
    {
        auto data = segment::Data::create(segment);
        if (auto ts = data->timestamp())
        {
            if (st_md->st_mtime < ts.value())
                nag::warning("%s: outdated .metadata file",
                             segment->abspath().c_str());
            return std::make_shared<segment::metadata::Reader>(segment, lock);
        }
        else
        {
            nag::warning("%s: segment data is not available",
                         segment->abspath().c_str());
            return std::make_shared<segment::EmptyReader>(segment, lock);
        }
    }
    else
        // Skip segment if .metadata does not exist
        return std::make_shared<segment::EmptyReader>(segment, lock);
}

std::shared_ptr<segment::Writer>
SegmentSession::segment_writer(std::shared_ptr<const Segment> segment,
                               std::shared_ptr<core::AppendLock> lock) const
{
    return std::make_shared<segment::metadata::Writer>(segment, lock);
}

std::shared_ptr<segment::Checker>
SegmentSession::segment_checker(std::shared_ptr<const Segment> segment,
                                std::shared_ptr<core::CheckLock> lock) const
{
    return std::make_shared<segment::metadata::Checker>(segment, lock);
}

Dataset::Dataset(std::shared_ptr<Session> session,
                 const core::cfg::Section& cfg)
    : dataset::segmented::Dataset(session,
                                  std::make_shared<SegmentSession>(cfg), cfg)
{
    if (cfg.value("index_type") == "sqlite")
        nag::warning("%s: dataset has index_type=sqlite. It is now ignored, "
                     "and automatically converted to plain MANIFEST",
                     name().c_str());
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<simple::Reader>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Writer> Dataset::create_writer()
{
    return std::make_shared<simple::Writer>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Checker> Dataset::create_checker()
{
    return std::make_shared<simple::Checker>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<core::ReadLock> Dataset::read_lock_dataset() const
{
    return std::make_shared<DatasetReadLock>(*this);
}

std::shared_ptr<core::AppendLock> Dataset::append_lock_dataset() const
{
    return std::make_shared<DatasetAppendLock>(*this);
}

std::shared_ptr<core::CheckLock> Dataset::check_lock_dataset() const
{
    return std::make_shared<DatasetCheckLock>(*this);
}

} // namespace simple
} // namespace dataset
} // namespace arki
