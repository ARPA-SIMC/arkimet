#include "empty.h"
#include "arki/metadata/collection.h"
#include "arki/utils/accounting.h"
#include "arki/core/time.h"
#include "session.h"
#include <ostream>

using namespace std;

namespace arki {
namespace dataset {
namespace empty {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader() { return std::make_shared<Reader>(shared_from_this()); }
std::shared_ptr<dataset::Writer> Dataset::create_writer() { return std::make_shared<Writer>(shared_from_this()); }
std::shared_ptr<dataset::Checker> Dataset::create_checker() { return std::make_shared<Checker>(shared_from_this()); }
core::Interval Reader::get_stored_time_interval() { return core::Interval(); }

WriterAcquireResult Writer::acquire(Metadata& md, const AcquireConfig& cfg)
{
    utils::acct::acquire_single_count.incr();
    return ACQ_OK;
}

void Writer::acquire_batch(WriterBatch& batch, const AcquireConfig& cfg)
{
    utils::acct::acquire_batch_count.incr();
    for (auto& e: batch)
    {
        e->result = ACQ_OK;
        e->dataset_name = name();
    }
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch)
{
    auto dataset = session->dataset(cfg);
    for (auto& e: batch)
    {
        e->result = ACQ_OK;
        e->dataset_name = dataset->name();
    }
}

}
}
}
