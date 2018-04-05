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

    bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;

    /**
     * Return true if this dataset has a working index.
     *
     * This method is mostly used for tests.
     */
    bool hasWorkingIndex() const { return m_idx != 0; }
};

class IndexedChecker : public segmented::Checker
{
protected:
    Index* m_idx = nullptr;

public:
    using segmented::Checker::Checker;
    ~IndexedChecker();

    const IndexedConfig& config() const override = 0;

    void check_issue51(CheckerConfig& opts) override;

    void test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx=1) override;
    void test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx=0) override;
    void test_corrupt_data(const std::string& relpath, unsigned data_idx=0) override;
    void test_truncate_data(const std::string& relpath, unsigned data_idx=0) override;
    void test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx) override;
    void test_rename(const std::string& relpath, const std::string& new_relpath) override;
};

}
}
#endif
