#ifndef ARKI_DATASET_LOCAL_H
#define ARKI_DATASET_LOCAL_H

/// dataset/local - Base class for local datasets

#include <arki/dataset.h>
#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/matcher/fwd.h>
#include <filesystem>
#include <string>

namespace arki::dataset {

namespace archive {
class Dataset;
class Reader;
class Checker;
}

namespace local {

class Dataset : public dataset::Dataset
{
protected:
    std::shared_ptr<archive::Dataset> m_archive;

public:
    /// Root path of the dataset
    std::filesystem::path path;

    int archive_age = -1;
    int delete_age = -1;

    const core::lock::Policy* lock_policy;

    Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg);

    /**
     * Check if the data to be acquired is older than acquire or delete age.
     *
     * If it is, return true and the import result value.
     *
     * If it is not, returns false and WriterAcquireResult should be ignored.
     */
    // TODO: change to use a metadata::Inbound
    std::pair<bool, metadata::Inbound::Result> check_acquire_age(Metadata& md) const;

    /// Return the Archives for this dataset
    virtual std::shared_ptr<archive::Dataset> archive();

    /// Check if the dataset has archived data
    bool hasArchive() const;
};

template<typename Parent>
class Base : public Parent
{
public:
    using Parent::Parent;

    const local::Dataset& dataset() const override = 0;
    local::Dataset& dataset() override = 0;
};

/**
 * Base class for local datasets
 */
class Reader : public Base<dataset::Reader>
{
    std::shared_ptr<dataset::Reader> m_archive;

protected:
    // Base implementations that queries the archives if they exist
    bool impl_query_data(const query::Data& q, metadata_dest_func dest) override;

    // Base implementations that queries the archives if they exist
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;

public:
    using Base::Base;
    ~Reader();

    /// Return the Reader for this dataset archives
    std::shared_ptr<dataset::Reader> archive();

    /// Read the configuration for the given dataset. path must point to a directory
    static std::shared_ptr<core::cfg::Section> read_config(const std::filesystem::path& path);

    static std::shared_ptr<core::cfg::Sections> read_configs(const std::filesystem::path& path);
};

class Writer : public Base<dataset::Writer>
{
public:
    using Base::Base;
    ~Writer();

    static void test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, metadata::InboundBatch& batch);
};

class Checker : public Base<dataset::Checker>
{
    std::shared_ptr<dataset::Checker> m_archive;

public:
    using Base::Base;
    ~Checker();

    /// Return the Checker for this dataset archives
    std::shared_ptr<dataset::Checker> archive();

    void repack(CheckerConfig& opts, unsigned test_flags=0) override;
    void check(CheckerConfig& opts) override;
    void remove_old(CheckerConfig& opts) override;
    void remove_all(CheckerConfig& opts) override;
    void check_issue51(CheckerConfig& opts) override;
    void tar(CheckerConfig& opts) override;
    void zip(CheckerConfig& opts) override;
    void compress(CheckerConfig& opts, unsigned gruopsize) override;
    void state(CheckerConfig& opts) override;
};

}
}
#endif
