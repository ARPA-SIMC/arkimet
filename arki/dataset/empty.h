#ifndef ARKI_DATASET_EMPTY_H
#define ARKI_DATASET_EMPTY_H

/// Virtual read only dataset that is always empty

#include <arki/dataset.h>
#include <string>

namespace arki {
class Metadata;
class Matcher;

namespace dataset {
namespace empty {

struct Config : public dataset::Config
{
    Config(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;
    std::unique_ptr<dataset::Checker> create_checker() const override;

    static std::shared_ptr<const Config> create(std::shared_ptr<Session> session, const core::cfg::Section& cfg);
};


/**
 * Dataset that is always empty
 */
class Reader : public dataset::Reader
{
protected:
    std::shared_ptr<const dataset::Config> m_config;

public:
    Reader(std::shared_ptr<const dataset::Config> config);
    virtual ~Reader();

    const dataset::Config& config() const override { return *m_config; }
    std::string type() const override { return "empty"; }

    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override { return true; }
    void query_summary(const Matcher& matcher, Summary& summary) override {}
    void query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out) override {}
    void query_bytes(const dataset::ByteQuery& q, core::AbstractOutputFile& out) override {}
};


/**
 * Writer that successfully discards all data
 */
class Writer : public dataset::Writer
{
protected:
    std::shared_ptr<const dataset::Config> m_config;

public:
    Writer(std::shared_ptr<const dataset::Config> config) : m_config(config) {}
    const dataset::Config& config() const override { return *m_config; }

    std::string type() const override { return "discard"; }

    WriterAcquireResult acquire(Metadata& md, const AcquireConfig& cfg=AcquireConfig()) override;
    void acquire_batch(WriterBatch& batch, const AcquireConfig& cfg=AcquireConfig()) override;

    void remove(Metadata&) override
    {
        // Of course, after this method is called, the metadata cannot be found
        // in the dataset
    }

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch);
};


/**
 * Checker that does nothing
 */
struct Checker : public dataset::Checker
{
protected:
    std::shared_ptr<const dataset::Config> m_config;

public:
    Checker(std::shared_ptr<const dataset::Config> config) : m_config(config) {}

    const dataset::Config& config() const override { return *m_config; }
    std::string type() const override { return "empty"; }

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
