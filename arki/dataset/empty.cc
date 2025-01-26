#include "empty.h"
#include "arki/utils/accounting.h"
#include "arki/core/time.h"
#include "session.h"

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

void Writer::acquire_batch(metadata::InboundBatch& batch, const AcquireConfig& cfg)
{
    utils::acct::acquire_batch_count.incr();
    for (auto& e: batch)
    {
        e->result = metadata::Inbound::Result::OK;
        e->destination = name();
    }
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, metadata::InboundBatch& batch)
{
    auto dataset = session->dataset(cfg);
    for (auto& e: batch)
    {
        e->result = metadata::Inbound::Result::OK;
        e->destination = dataset->name();
    }
}

}
}
}
