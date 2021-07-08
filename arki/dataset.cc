#include "arki/dataset.h"
#include "arki/dataset/query.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/session.h"
#include "arki/dataset/outbound.h"
#include "arki/dataset/empty.h"
#include "arki/metadata.h"
#include "arki/metadata/postprocess.h"
#include "arki/utils/string.h"
#include "arki/stream.h"
#include "arki/summary.h"

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {

Dataset::Dataset(std::shared_ptr<Session> session) : session(session) {}

Dataset::Dataset(std::shared_ptr<Session> session, const std::string& name) : m_name(name), session(session) {}

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : m_name(cfg.value("name")), session(session), config(std::make_shared<core::cfg::Section>(cfg))
{
}

std::string Dataset::name() const
{
    if (m_parent)
        return m_parent->name() + "." + m_name;
    else
        return m_name;
}

void Dataset::set_parent(const Dataset* parent)
{
    m_parent = parent;
}

std::shared_ptr<Reader> Dataset::create_reader() { throw std::runtime_error("reader not implemented for dataset " + name()); }
std::shared_ptr<Writer> Dataset::create_writer() { throw std::runtime_error("writer not implemented for dataset " + name()); }
std::shared_ptr<Checker> Dataset::create_checker() { throw std::runtime_error("checker not implemented for dataset " + name()); }


void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    query_data(DataQuery(matcher), [&](std::shared_ptr<Metadata> md) { summary.add(*md); return true; });
}

void Reader::impl_stream_query_bytes(const dataset::ByteQuery& q, StreamOutput& out)
{
    switch (q.type)
    {
        case dataset::ByteQuery::BQ_DATA: {
            query_data(q, [&](std::shared_ptr<Metadata> md) {
                auto res = md->stream_data(out);
                return !(res.flags & stream::SendResult::SEND_PIPE_EOF_DEST);
            });
            break;
        }
        case dataset::ByteQuery::BQ_POSTPROCESS: {
            metadata::Postprocess postproc(q.postprocessor, out);
            postproc.validate(*dataset().config);
            postproc.start();
            query_data(q, [&](std::shared_ptr<Metadata> md) { return postproc.process(move(md)); });
            postproc.flush();
            break;
        }
        default:
        {
            stringstream s;
            s << "cannot query dataset: unsupported query type: " << (int)q.type;
            throw std::runtime_error(s.str());
        }
    }
}

bool Reader::query_data(const std::string& q, metadata_dest_func dest)
{
    dataset::DataQuery dq(dataset().session->matcher(q));
    return impl_query_data(dq, dest);
}

void Reader::query_summary(const std::string& matcher, Summary& summary)
{
    impl_query_summary(dataset().session->matcher(matcher), summary);
}


void WriterBatch::set_all_error(const std::string& note)
{
    for (auto& e: *this)
    {
        e->dataset_name.clear();
        e->md.add_note(note);
        e->result = ACQ_ERROR;
    }
}


void Writer::flush() {}

Pending Writer::test_writelock() { return Pending(); }

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch)
{
    string type = str::lower(cfg.value("type"));
    if (type == "remote")
        throw std::runtime_error("cannot simulate dataset acquisition: remote datasets are not writable");
    if (type == "outbound")
        return outbound::Writer::test_acquire(session, cfg, batch);
    if (type == "discard")
        return empty::Writer::test_acquire(session, cfg, batch);

    return dataset::local::Writer::test_acquire(session, cfg, batch);
}

CheckerConfig::CheckerConfig()
    : reporter(make_shared<NullReporter>())
{
}

CheckerConfig::CheckerConfig(std::shared_ptr<dataset::Reporter> reporter, bool readonly)
    : reporter(reporter), readonly(readonly)
{
}


void Checker::check_issue51(CheckerConfig& opts)
{
    throw std::runtime_error(dataset().name() + ": check_issue51 not implemented for this dataset");
}


}
}
