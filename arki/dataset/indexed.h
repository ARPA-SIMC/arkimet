#ifndef ARKI_DATASET_INDEXED_H
#define ARKI_DATASET_INDEXED_H

#include <arki/dataset/segmented.h>

namespace arki {
namespace dataset {
class Index;

struct IndexedConfig : public segmented::Config
{
    using segmented::Config::Config;
};


/// segmented::Reader that can make use of an index
class IndexedReader : public segmented::Reader
{
protected:
    Index* m_idx = nullptr;

public:
    using segmented::Reader::Reader;
    ~IndexedReader();

    const IndexedConfig& config() const override = 0;

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;

    /**
     * Return true if this dataset has a working index.
     *
     * This method is mostly used for tests.
     */
    bool hasWorkingIndex() const { return m_idx != 0; }
};

class IndexedWriter : public segmented::Writer
{
protected:
    Index* m_idx = nullptr;

public:
    using segmented::Writer::Writer;
    ~IndexedWriter();

    const IndexedConfig& config() const override = 0;
};

class IndexedChecker : public segmented::Checker
{
protected:
    Index* m_idx = nullptr;

public:
    using segmented::Checker::Checker;
    ~IndexedChecker();

    const IndexedConfig& config() const override = 0;

    segmented::State scan(dataset::Reporter& reporter, bool quick=true) override;
    void removeAll(Reporter& reporter, bool writable) override;
    void check_issue51(dataset::Reporter& reporter, bool fix=false) override;
};

/**
 * Temporarily override the current date used to check data age.
 *
 * This is used to be able to write unit tests that run the same independently
 * of when they are run.
 */
struct TestOverrideCurrentDateForMaintenance
{
    time_t old_ts;

    TestOverrideCurrentDateForMaintenance(time_t ts);
    ~TestOverrideCurrentDateForMaintenance();
};

}
}
#endif
