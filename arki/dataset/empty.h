#ifndef ARKI_DATASET_EMPTY_H
#define ARKI_DATASET_EMPTY_H

/// Virtual read only dataset that is always empty

#include <arki/dataset.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {
namespace empty {

struct Config : public dataset::Config
{
    Config(const ConfigFile& cfg);

    std::unique_ptr<dataset::Reader> create_reader() const override;
    std::unique_ptr<dataset::Writer> create_writer() const override;
    std::unique_ptr<dataset::Checker> create_checker() const override;

    static std::shared_ptr<const Config> create(const ConfigFile& cfg);
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

    // Nothing to do: the dataset is always empty
    bool query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)>) override { return true; }
    void query_summary(const Matcher& matcher, Summary& summary) override {}
    void query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out) override {}
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

    WriterAcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace=REPLACE_DEFAULT) override;

    void remove(Metadata&) override
    {
        // Of course, after this method is called, the metadata cannot be found
        // in the dataset
    }

    static void test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out);
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

    void remove_all(dataset::Reporter& reporter, bool writable=false) override {}
    void remove_all_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false) override {}
    void repack(dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override {}
    void check(dataset::Reporter& reporter, bool fix, bool quick) override {}
    void repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable=false, unsigned test_flags=0) override {}
    void check_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool fix, bool quick) override {}
};


}
}
}
#endif
