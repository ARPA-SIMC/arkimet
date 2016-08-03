#include "sharded.h"
#include "step.h"
#include "simple.h"
#include "ondisk2.h"
#include "reporter.h"
#include "archive.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils/compress.h"
#include "arki/scan/any.h"
#include "arki/metadata/collection.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

namespace sharded {

template<typename Base>
Config<Base>::Config(const ConfigFile& cfg)
    : Base(cfg), sharded(!cfg.value("shard").empty())
{
    std::string shard = cfg.value("shard");
    if (!shard.empty())
        shard_step = ShardStep::create(shard, cfg.value("step"));
}

template<typename Base>
void Config<Base>::all_shards(std::function<void(const std::string&, std::shared_ptr<const dataset::Config>)> f) const
{
    auto shards = shard_step->list_shards(this->path);
    for (const auto& t: shards)
    {
        auto shard = create_shard(t.first);
        f(shard.first, shard.second);
    }
}

template<typename Base>
void Config<Base>::query_shards(const Matcher& matcher, std::function<void(const std::string&, std::shared_ptr<const dataset::Config>)> f) const
{
    auto shards = shard_step->list_shards(this->path);
    unique_ptr<core::Time> begin;
    unique_ptr<core::Time> end;
    if (!matcher.restrict_date_range(begin, end))
        // The matcher matches an impossible reftime span: return right away
        return;
    for (const auto& t: shards)
    {
        if (begin && *begin > t.second) continue;
        if (end && *end < t.first) continue;
        auto shard = create_shard(t.first);
        f(shard.first, shard.second);
    }
}

template<typename Base>
void Config<Base>::to_shard(const std::string& shard_path, std::shared_ptr<Step> step)
{
    Base::to_shard(shard_path, step);
    sharded = false;
    shard_step = std::shared_ptr<ShardStep>();
}

template<typename Config>
Reader<Config>::Reader(std::shared_ptr<const Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

template<typename Config>
Reader<Config>::~Reader() {}

template<typename Config>
void Reader<Config>::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    config().query_shards(q.matcher, [&](const std::string& shard_relpath, std::shared_ptr<const dataset::Config> cfg) {
        cfg->create_reader()->query_data(q, dest);
    });
}

template<typename Config>
void Reader<Config>::query_summary(const Matcher& matcher, Summary& summary)
{
    config().query_shards(matcher, [&](const std::string& shard_relpath, std::shared_ptr<const dataset::Config> cfg) {
        cfg->create_reader()->query_summary(matcher, summary);
    });
}

template<typename Config>
void Reader<Config>::expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end)
{
    config().all_shards([&](const std::string& shard_relpath, std::shared_ptr<const dataset::Config> cfg) {
        cfg->create_reader()->expand_date_range(begin, end);
    });
}


template<typename Config>
Writer<Config>::Writer(std::shared_ptr<const Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

template<typename Config>
Writer<Config>::~Writer()
{
    for (auto& i: shards)
        delete i.second;
}

template<typename Config>
dataset::Writer& Writer<Config>::shard(const core::Time& time)
{
    std::string shard_path = config().shard_step->shard_path(time);
    auto res = shards.find(shard_path);
    if (res == shards.end())
    {
        auto shard = m_config->create_shard(time);
        auto writer = shard.second->create_writer();
        auto i = shards.emplace(make_pair(shard_path, writer.release()));
        return *i.first->second;
    } else
        return *res->second;
}

template<typename Config>
dataset::Writer::AcquireResult Writer<Config>::acquire(Metadata& md, dataset::Writer::ReplaceStrategy replace)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    auto res = shard(time).acquire(md, replace);
    const auto& source = md.sourceBlob();
    md.set_source(source.makeRelativeTo(path()));
    return res;
}

template<typename Config>
void Writer<Config>::remove(Metadata& md)
{
    for (auto& i: shards)
        delete i.second;
    const core::Time& time = md.get<types::reftime::Position>()->time;
    return shard(time).remove(md);
}

template<typename Config>
void Writer<Config>::flush()
{
    // Flush all shards
    for (auto& i: shards)
        i.second->flush();

    // Deallocate all cached shards
    for (auto& i: shards)
        delete i.second;
    shards.clear();
}


template<typename Config>
Checker<Config>::Checker(std::shared_ptr<const Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

template<typename Config>
Checker<Config>::~Checker()
{
    for (auto& i: shards)
        delete i.second;
}

template<typename Config>
segmented::Checker& Checker<Config>::shard(const core::Time& time)
{
    std::string shard_path = config().shard_step->shard_path(time);
    auto res = shards.find(shard_path);
    if (res == shards.end())
    {
        auto shard = m_config->create_shard(time);
        auto checker = shard.second->create_checker();
        segmented::Checker* sc = dynamic_cast<segmented::Checker*>(checker.get());
        if (!sc) throw std::runtime_error("shard dataset for " + shard_path + " did not create a segmented checker");
        auto i = shards.emplace(make_pair(shard_path, (checker.release(), sc)));
        return *i.first->second;
    } else
        return *res->second;
}

template<typename Config>
segmented::Checker& Checker<Config>::shard(const std::string& shard_path)
{
    auto res = shards.find(shard_path);
    if (res == shards.end())
    {
        auto span = config().shard_step->shard_span(shard_path);
        auto shard = m_config->create_shard(span.first);
        auto checker = shard.second->create_checker();
        segmented::Checker* sc = dynamic_cast<segmented::Checker*>(checker.get());
        auto i = shards.emplace(make_pair(shard_path, (checker.release(), sc)));
        return *i.first->second;
    } else
        return *res->second;
}

template<typename Config>
segmented::Checker& Checker<Config>::shard(const std::string& shard_path, std::shared_ptr<const dataset::Config> config)
{
    auto res = shards.find(shard_path);
    if (res == shards.end())
    {
        auto checker = config->create_checker();
        segmented::Checker* sc = dynamic_cast<segmented::Checker*>(checker.get());
        auto i = shards.emplace(make_pair(shard_path, (checker.release(), sc)));
        return *i.first->second;
    } else
        return *res->second;
}

namespace {

struct ShardedReporter : public dataset::Reporter
{
    dataset::Reporter& orig;
    std::string shard_relpath;

    ShardedReporter(dataset::Reporter& orig, const std::string& shard_relpath) : orig(orig), shard_relpath(shard_relpath) {}

    void operation_progress(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        orig.operation_progress(ds, operation, shard_relpath + ": " + message);
    }
    void operation_manual_intervention(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        orig.operation_manual_intervention(ds, operation, shard_relpath + ": " + message);
    }
    void operation_aborted(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        orig.operation_aborted(ds, operation, shard_relpath + ": " + message);
    }
    void operation_report(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        orig.operation_report(ds, operation, shard_relpath + ": " + message);
    }
    void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        orig.segment_info(ds, str::joinpath(shard_relpath, relpath), message);
    }
    void segment_repack(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        orig.segment_repack(ds, str::joinpath(shard_relpath, relpath), message);
    }
    void segment_archive(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        orig.segment_archive(ds, str::joinpath(shard_relpath, relpath), message);
    }
    void segment_delete(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        orig.segment_delete(ds, str::joinpath(shard_relpath, relpath), message);
    }
    void segment_deindex(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        orig.segment_deindex(ds, str::joinpath(shard_relpath, relpath), message);
    }
    void segment_rescan(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        orig.segment_rescan(ds, str::joinpath(shard_relpath, relpath), message);
    }
};

}

template<typename Config>
void Checker<Config>::removeAll(dataset::Reporter& reporter, bool writable)
{
    config().all_shards([&](const std::string& shard_relpath, std::shared_ptr<const dataset::Config> cfg) {
        ShardedReporter rep(reporter, shard_relpath);
        shard(shard_relpath, cfg).removeAll(rep, writable);
    });
}

template<typename Config>
segmented::State Checker<Config>::scan(dataset::Reporter& reporter, bool quick)
{
    segmented::State res;

    config().all_shards([&](const std::string& shard_relpath, std::shared_ptr<const dataset::Config> cfg) {
        ShardedReporter rep(reporter, shard_relpath);
        segmented::State partial = shard(shard_relpath, cfg).scan(reporter, quick);
        for (const auto& i: partial)
            res.insert(make_pair(str::joinpath(shard_relpath, i.first), i.second));
    });

    return res;
}

template<typename Config>
void Checker<Config>::indexSegment(const std::string& relpath, metadata::Collection&& contents)
{
    size_t pos = relpath.find('/');
    if (pos == string::npos) throw std::runtime_error("path " + relpath + " does not contain a /");
    shard(relpath.substr(0, pos)).indexSegment(relpath.substr(pos + 1), move(contents));
}

template<typename Config>
void Checker<Config>::rescanSegment(const std::string& relpath)
{
    size_t pos = relpath.find('/');
    if (pos == string::npos) throw std::runtime_error("path " + relpath + " does not contain a /");
    shard(relpath.substr(0, pos)).rescanSegment(relpath.substr(pos + 1));
}

template<typename Config>
size_t Checker<Config>::repackSegment(const std::string& relpath)
{
    size_t pos = relpath.find('/');
    if (pos == string::npos) throw std::runtime_error("path " + relpath + " does not contain a /");
    return shard(relpath.substr(0, pos)).repackSegment(relpath.substr(pos + 1));
}

template<typename Config>
size_t Checker<Config>::removeSegment(const std::string& relpath, bool withData)
{
    size_t pos = relpath.find('/');
    if (pos == string::npos) throw std::runtime_error("path " + relpath + " does not contain a /");
    return shard(relpath.substr(0, pos)).removeSegment(relpath.substr(pos + 1));
}

template<typename Config>
void Checker<Config>::releaseSegment(const std::string& relpath, const std::string& destpath)
{
    size_t pos = relpath.find('/');
    if (pos == string::npos) throw std::runtime_error("path " + relpath + " does not contain a /");
    return shard(relpath.substr(0, pos)).releaseSegment(relpath.substr(pos + 1), destpath);
}

template<typename Config>
size_t Checker<Config>::vacuum()
{
    size_t res = 0;
    config().all_shards([&](const std::string& shard_relpath, std::shared_ptr<const dataset::Config> cfg) {
        res += shard(shard_relpath, cfg).vacuum();
    });
    return res;
}


template class Config<dataset::IndexedConfig>;
template class Reader<simple::Config>;
template class Reader<ondisk2::Config>;
template class Writer<simple::Config>;
template class Writer<ondisk2::Config>;
template class Checker<simple::Config>;
template class Checker<ondisk2::Config>;

}
}
}
