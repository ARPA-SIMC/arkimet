#ifndef ARKI_DATASET_MEMORY_H
#define ARKI_DATASET_MEMORY_H

/// dataset/memory - Dataset interface for metadata::Collection

#include <arki/metadata/collection.h>
#include <arki/dataset.h>

namespace arki {
struct Summary;

namespace sort {
struct Compare;
}

namespace dataset {

/**
 * Consumer that collects all metadata into a vector
 */
struct Memory : public metadata::Collection, public Reader
{
public:
    Memory();
    virtual ~Memory();

    std::string type() const override;
    void query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

}
}

#endif

