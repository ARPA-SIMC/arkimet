#include "simple.h"
#include "simple/reader.h"
#include "simple/writer.h"
#include "simple/checker.h"
#include "arki/dataset/lock.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::segmented::Dataset(session, cfg),
      index_type(cfg.value("index_type"))
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<simple::Reader>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Writer> Dataset::create_writer()
{
    return std::make_shared<simple::Writer>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Checker> Dataset::create_checker()
{
    return std::make_shared<simple::Checker>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::ReadLock> Dataset::read_lock_dataset() const
{
    return std::make_shared<DatasetReadLock>(*this);
}

std::shared_ptr<dataset::AppendLock> Dataset::append_lock_dataset() const
{
    return std::make_shared<DatasetAppendLock>(*this);
}

std::shared_ptr<dataset::CheckLock> Dataset::check_lock_dataset() const
{
    return std::make_shared<DatasetCheckLock>(*this);
}

}
}
}
