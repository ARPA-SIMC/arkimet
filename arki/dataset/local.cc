#include "local.h"
#include "lock.h"
#include "segmented.h"
#include "archive.h"
#include "arki/dataset/time.h"
#include "arki/types/reftime.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/utils/files.h"
#include "arki/metadata/consumer.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <fcntl.h>
#include <sys/stat.h>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {

LocalConfig::LocalConfig(const core::cfg::Section& cfg)
    : Config(cfg), path(sys::abspath(cfg.value("path")))
{
    string tmp = cfg.value("archive age");
    if (!tmp.empty())
        archive_age = std::stoi(tmp);

    tmp = cfg.value("delete age");
    if (!tmp.empty())
        delete_age = std::stoi(tmp);

    if (cfg.value("locking") == "no")
        lock_policy = core::lock::policy_null;
    else
        lock_policy = core::lock::policy_ofd;
}

std::pair<bool, WriterAcquireResult> LocalConfig::check_acquire_age(Metadata& md) const
{
    const auto& st = SessionTime::get();
    const types::reftime::Position* rt = md.get<types::reftime::Position>();

    if (delete_age != -1 && rt->time < st.age_threshold(delete_age))
    {
        md.add_note("Safely discarded: data is older than delete age");
        return make_pair(true, ACQ_OK);
    }

    if (archive_age != -1 && rt->time < st.age_threshold(archive_age))
    {
        md.add_note("Import refused: data is older than archive age");
        return make_pair(true, ACQ_ERROR);
    }

    return make_pair(false, ACQ_OK);
}

std::shared_ptr<ArchivesConfig> LocalConfig::archives_config() const
{
    if (!m_archives_config)
        m_archives_config = std::shared_ptr<ArchivesConfig>(new ArchivesConfig(path));
    return m_archives_config;
}

std::shared_ptr<dataset::ReadLock> LocalConfig::read_lock_dataset() const
{
    return std::make_shared<DatasetReadLock>(*this);
}

std::shared_ptr<dataset::AppendLock> LocalConfig::append_lock_dataset() const
{
    return std::make_shared<DatasetAppendLock>(*this);
}

std::shared_ptr<dataset::CheckLock> LocalConfig::check_lock_dataset() const
{
    return std::make_shared<DatasetCheckLock>(*this);
}

std::shared_ptr<dataset::ReadLock> LocalConfig::read_lock_segment(const std::string& relpath) const
{
    return std::make_shared<SegmentReadLock>(*this, relpath);
}

std::shared_ptr<dataset::AppendLock> LocalConfig::append_lock_segment(const std::string& relpath) const
{
    return std::make_shared<SegmentAppendLock>(*this, relpath);
}

std::shared_ptr<dataset::CheckLock> LocalConfig::check_lock_segment(const std::string& relpath) const
{
    return std::make_shared<SegmentCheckLock>(*this, relpath);
}


template<typename Parent, typename Archive>
LocalBase<Parent, Archive>::~LocalBase()
{
    delete m_archive;
}

template<typename Parent, typename Archive>
bool LocalBase<Parent, Archive>::hasArchive() const
{
    string arcdir = str::joinpath(path(), ".archive");
    return sys::exists(arcdir);
}

template<typename Parent, typename Archive>
Archive& LocalBase<Parent, Archive>::archive()
{
    if (!m_archive)
    {
        m_archive = new Archive(config().archives_config());
        m_archive->set_parent(*this);
    }
    return *m_archive;
}


LocalReader::~LocalReader()
{
}

bool LocalReader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    if (!hasArchive()) return true;
    return archive().query_data(q, dest);
}

void LocalReader::query_summary(const Matcher& matcher, Summary& summary)
{
    if (hasArchive())
        archive().query_summary(matcher, summary);
}

core::cfg::Section LocalReader::read_config(const std::string& path)
{
    if (path == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Section::parse(in);
    }

    // Remove trailing slashes, if any
    string fname = path;
    while (!fname.empty() && fname[fname.size() - 1] == '/')
        fname.resize(fname.size() - 1);

    // Check if it's a file or a directory
    std::unique_ptr<struct stat> st = sys::stat(fname);
    if (st.get() == 0)
    {
        stringstream ss;
        ss << "cannot read configuration from " << fname << " because it does not exist";
        throw runtime_error(ss.str());
    }
    if (S_ISDIR(st->st_mode))
    {
        // If it's a directory, merge in its config file
        string name = str::basename(fname);
        string file = str::joinpath(fname, "config");

        File in(file, O_RDONLY);
        // Parse the config file into a new section
        auto res = core::cfg::Section::parse(in);
        // Fill in missing bits
        res.set("name", name);
        res.set("path", sys::abspath(fname));
        return res;
    } else {
        // If it's a file, then it's the config file for one dataset
        File in(fname, O_RDONLY);
        // Parse the config file
        return core::cfg::Section::parse(in);
    }
}

core::cfg::Sections LocalReader::read_configs(const std::string& path)
{
    if (path == "-")
    {
        // Parse the config file from stdin
        Stdin in;
        return core::cfg::Sections::parse(in);
    }

    // Remove trailing slashes, if any
    string fname = path;
    while (!fname.empty() && fname[fname.size() - 1] == '/')
        fname.resize(fname.size() - 1);

    File in(fname, O_RDONLY);
    return core::cfg::Sections::parse(in);
}


LocalWriter::~LocalWriter()
{
}

void LocalWriter::test_acquire(const core::cfg::Section& cfg, WriterBatch& batch)
{
    return segmented::Writer::test_acquire(cfg, batch);
}


LocalChecker::~LocalChecker()
{
}

void LocalChecker::repack(CheckerConfig& opts, unsigned test_flags)
{
    if (opts.offline && hasArchive())
        archive().repack(opts, test_flags);
}

void LocalChecker::check(CheckerConfig& opts)
{
    if (opts.offline && hasArchive())
        archive().check(opts);
}

void LocalChecker::remove_old(CheckerConfig& opts)
{
    if (opts.offline && hasArchive())
        archive().remove_old(opts);
}

void LocalChecker::remove_all(CheckerConfig& opts)
{
    if (opts.offline && hasArchive())
        archive().remove_all(opts);
}

void LocalChecker::tar(CheckerConfig& opts)
{
    if (opts.offline && hasArchive())
        archive().tar(opts);
}

void LocalChecker::zip(CheckerConfig& opts)
{
    if (opts.offline && hasArchive())
        archive().zip(opts);
}

void LocalChecker::compress(CheckerConfig& opts, unsigned groupsize)
{
    if (opts.offline && hasArchive())
        archive().compress(opts, groupsize);
}

void LocalChecker::check_issue51(CheckerConfig& opts)
{
    if (opts.offline && hasArchive())
        archive().check_issue51(opts);
}

void LocalChecker::state(CheckerConfig& opts)
{
    if (opts.offline && hasArchive())
        archive().state(opts);
}

template class LocalBase<Reader, ArchivesReader>;
template class LocalBase<Checker, ArchivesChecker>;
}
}
