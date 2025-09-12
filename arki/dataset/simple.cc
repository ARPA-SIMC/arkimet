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

namespace arki::dataset::simple {

/*
 * Dataset
 */

Dataset::Dataset(std::shared_ptr<Session> session,
                 const core::cfg::Section& cfg)
    : dataset::segmented::Dataset(
          session, std::make_shared<segment::metadata::Session>(cfg), cfg)
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

} // namespace arki::dataset::simple
