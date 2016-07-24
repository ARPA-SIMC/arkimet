#ifndef ARKI_DATASET_SHARD_H
#define ARKI_DATASET_SHARD_H

#include <arki/dataset/local.h>

namespace arki {
namespace dataset {
class ShardStep;

namespace simple { class Config; }
namespace ondisk2 { class Config; }

struct ShardingConfig
{
    bool active = false;
    std::shared_ptr<ShardStep> step;

    ShardingConfig(const ConfigFile& cfg);

    std::unique_ptr<Reader> create_shard_reader(const core::Time& time) const;
    std::unique_ptr<Writer> create_shard_writer(const core::Time& time) const;
    std::unique_ptr<Checker> create_shard_checker(const core::Time& time) const;
};

namespace sharded {

template<typename Config>
class Reader : public LocalReader
{
protected:
    std::shared_ptr<const Config> m_config;
    const ShardingConfig& sharding;

public:
    Reader(std::shared_ptr<const Config> config);
    ~Reader();

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) override;
};

template<typename Config>
class Writer : public LocalWriter
{
protected:
    std::shared_ptr<const Config> m_config;

public:
    Writer(std::shared_ptr<const Config> config);
    ~Writer();

    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md) override;
    void flush() override;

};

template<typename Config>
class Checker : public LocalChecker
{
protected:
    std::shared_ptr<const Config> m_config;

public:
    Checker(std::shared_ptr<const Config> config);
    ~Checker();

    void removeAll(dataset::Reporter& reporter, bool writable=false) override;
    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
};

extern template class Reader<simple::Config>;
extern template class Reader<ondisk2::Config>;
extern template class Writer<simple::Config>;
extern template class Writer<ondisk2::Config>;
extern template class Checker<simple::Config>;
extern template class Checker<ondisk2::Config>;

}
}
}
#endif
