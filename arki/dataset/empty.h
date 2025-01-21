#ifndef ARKI_DATASET_EMPTY_H
#define ARKI_DATASET_EMPTY_H

/// Virtual read only dataset that is always empty

#include <arki/dataset.h>
#include <arki/dataset/impl.h>
#include <string>

namespace arki {
class Metadata;
class Matcher;

namespace dataset {
namespace empty {

struct Dataset : public dataset::Dataset
{
    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Writer> create_writer() override;
    std::shared_ptr<dataset::Checker> create_checker() override;
};


/**
 * Dataset that is always empty
 */
class Reader : public DatasetAccess<dataset::Dataset, dataset::Reader>
{
protected:
    bool impl_query_data(const query::Data& q, metadata_dest_func) override { return true; }
    void impl_query_summary(const Matcher& matcher, Summary& summary) override {}
    void impl_stream_query_bytes(const query::Bytes& q, StreamOutput& out) override {}

public:
    using DatasetAccess::DatasetAccess;

    std::string type() const override { return "empty"; }

    core::Interval get_stored_time_interval() override;
};


/**
 * Writer that successfully discards all data
 */
class Writer : public DatasetAccess<dataset::Dataset, dataset::Writer>
{
public:
    using DatasetAccess::DatasetAccess;

    std::string type() const override { return "discard"; }

    WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) override;
    void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch);
};


/**
 * Checker that does nothing
 */
struct Checker : public DatasetAccess<dataset::Dataset, dataset::Checker>
{
public:
    using DatasetAccess::DatasetAccess;

    std::string type() const override { return "empty"; }

    void remove(const metadata::Collection& mds) override {}
    void remove_old(CheckerConfig& opts) override {}
    void remove_all(CheckerConfig& opts) override {}
    void tar(CheckerConfig&) override {}
    void zip(CheckerConfig&) override {}
    void compress(CheckerConfig&, unsigned groupsize) override {}
    void repack(CheckerConfig&, unsigned test_flags=0) override {}
    void check(CheckerConfig&) override {}
    void state(CheckerConfig&) override {}
};


}
}
}
#endif
