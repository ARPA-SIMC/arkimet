#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/summary.h"
#include <ctime>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace simple {

/// Accumulate metadata and summaries while writing
struct AppendSegment
{
    std::shared_ptr<const simple::Config> config;
    std::shared_ptr<dataset::AppendLock> lock;
    std::shared_ptr<segment::Writer> segment;
    utils::sys::Path dir;
    std::string basename;
    bool flushed = true;
    metadata::Collection mds;
    Summary sum;

    AppendSegment(std::shared_ptr<const simple::Config> config, std::shared_ptr<dataset::AppendLock> lock, std::shared_ptr<segment::Writer> segment)
        : config(config), lock(lock), segment(segment),
          dir(str::dirname(segment->absname)),
          basename(str::basename(segment->absname))
    {
        struct stat st_data;
        if (!dir.fstatat_ifexists(basename.c_str(), st_data))
            return;

        // Read the metadata
        scan::scan(segment->absname, lock, mds.inserter_func());

        // Read the summary
        if (!mds.empty())
            mds.add_to_summary(sum);
    }

    ~AppendSegment()
    {
        flush();
    }

    void add(const Metadata& md, const types::source::Blob& source)
    {
        using namespace arki::types;

        // Replace the pathname with its basename
        unique_ptr<Metadata> copy(md.clone());
        copy->set_source(Source::createBlobUnlocked(source.format, dir.name(), basename, source.offset, source.size));
        sum.add(*copy);
        mds.acquire(move(copy));
        flushed = false;
    }

    void flush()
    {
        if (flushed) return;
        mds.writeAtomically(segment->absname + ".metadata");
        sum.writeAtomically(segment->absname + ".summary");
        //fsync(dir);
    }

    WriterAcquireResult acquire(Metadata& md)
    {
        auto mft = index::Manifest::create(config->path, config->lock_policy, config->index_type);
        mft->lock = lock;
        mft->openRW();

        // Try appending
        try {
            const types::source::Blob& new_source = segment->append(md);
            add(md, new_source);
            time_t ts = scan::timestamp(segment->absname);
            if (ts == 0)
                nag::warning("simple dataset acquire: %s timestamp is 0", segment->absname.c_str());
            mft->acquire(segment->relname, ts, sum);
            segment->commit();
            mft->flush();
            return ACQ_OK;
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    void acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch)
    {
        auto mft = index::Manifest::create(config->path, config->lock_policy, config->index_type);
        mft->lock = lock;
        mft->openRW();

        for (auto& e: batch)
        {
            e->dataset_name.clear();
            const types::source::Blob& new_source = segment->append(e->md);
            add(e->md, new_source);
            time_t ts = scan::timestamp(segment->absname);
            if (ts == 0)
                nag::warning("simple dataset acquire: %s timestamp is 0", segment->absname.c_str());
            mft->acquire(segment->relname, ts, sum);
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }

        segment->commit();
        mft->flush();
    }
};


Writer::Writer(std::shared_ptr<const simple::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(config->path))
        files::createDontpackFlagfile(config->path);
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "simple"; }

std::unique_ptr<AppendSegment> Writer::file(const Metadata& md, const std::string& format)
{
    auto lock = config().append_lock_dataset();
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_config, lock, segmented::Writer::file(md, format)));
}

std::unique_ptr<AppendSegment> Writer::file(const std::string& relname)
{
    sys::makedirs(str::dirname(str::joinpath(config().path, relname)));
    auto lock = config().append_lock_dataset();
    auto segment = segment_manager().get_writer(relname);
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_config, lock, segment));
}

WriterAcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    auto segment = file(md, md.source().format);
    return segment->acquire(md);
}

void Writer::acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace)
{
    if (replace == REPLACE_DEFAULT) replace = config().default_replace_strategy;

    // Clear dataset names, pre-process items that do not need further
    // dispatching, and divide the rest of the batch by segment
    std::map<std::string, std::vector<std::shared_ptr<dataset::WriterBatchElement>>> by_segment;
    for (auto& e: batch)
    {
        e->dataset_name.clear();

        switch (replace)
        {
            case REPLACE_NEVER:
            case REPLACE_ALWAYS:
            case REPLACE_HIGHER_USN: break;
            default:
            {
                e->md.add_note("cannot acquire into dataset " + name() + ": replace strategy " + std::to_string(replace) + " is not supported");
                e->result = ACQ_ERROR;
                continue;
            }
        }

        auto age_check = config().check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = name();
            continue;
        }

        const core::Time& time = e->md.get<types::reftime::Position>()->time;
        string relname = config().step()(time) + "." + e->md.source().format;
        by_segment[relname].push_back(e);
    }

    // Import segment by segment
    for (auto& s: by_segment)
    {
        auto segment = file(s.first);
        segment->acquire_batch(s.second);
    }
}

void Writer::remove(Metadata& md)
{
    // Nothing to do
    throw std::runtime_error("cannot remove data from simple dataset: dataset does not support removing items");
}

void Writer::test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out)
{
    std::shared_ptr<const simple::Config> config(new simple::Config(cfg));
    for (auto& e: batch)
    {
        auto age_check = config->check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = config->name;
            else
                e->dataset_name.clear();
        } else {
            // Acquire on simple datasets always succeeds except in case of envrionment
            // issues like I/O errors and full disks
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }
    }
}

}
}
}
