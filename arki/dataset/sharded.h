#ifndef ARKI_DATASET_SHARD_H
#define ARKI_DATASET_SHARD_H

#include <arki/dataset/local.h>

namespace arki {
namespace dataset {
class Step;

namespace sharded {

class Reader : public LocalReader
{
public:
    ~Reader();

    std::string type() const override;
    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;
};

class Writer : public LocalWriter
{
public:
    ~Writer();

    std::string type() const override;
    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md) override;
    void flush() override;

};

class Checker : public LocalChecker
{
public:
    ~Checker();

    std::string type() const override;
    void removeAll(dataset::Reporter& reporter, bool writable=false) override;
    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
};

}
}
}
#endif
