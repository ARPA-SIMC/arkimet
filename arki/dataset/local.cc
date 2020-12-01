#include "local.h"
#include "lock.h"
#include "segmented.h"
#include "archive.h"
#include "arki/dataset/time.h"
#include "arki/types/reftime.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/utils/files.h"
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
namespace local {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg), path(sys::abspath(cfg.value("path")))
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

std::pair<bool, WriterAcquireResult> Dataset::check_acquire_age(Metadata& md) const
{
    const auto& st = SessionTime::get();
    const types::reftime::Position* rt = md.get<types::reftime::Position>();
    auto time = rt->get_Position();

    if (delete_age != -1 && time < st.age_threshold(delete_age))
    {
        md.add_note("Safely discarded: data is older than delete age");
        return make_pair(true, ACQ_OK);
    }

    if (archive_age != -1 && time < st.age_threshold(archive_age))
    {
        md.add_note("Import refused: data is older than archive age");
        return make_pair(true, ACQ_ERROR);
    }

    return make_pair(false, ACQ_OK);
}

std::shared_ptr<archive::Dataset> Dataset::archive()
{
    if (!m_archive)
    {
        m_archive = std::shared_ptr<archive::Dataset>(new archive::Dataset(session, path));
        m_archive->set_parent(shared_from_this());
    }
    return m_archive;
}

bool Dataset::hasArchive() const
{
    string arcdir = str::joinpath(path, ".archive");
    return sys::exists(arcdir);
}

std::shared_ptr<dataset::ReadLock> Dataset::read_lock_dataset() const
{
    return std::make_shared<DatasetReadLock>(*this);
}

std::shared_ptr<dataset::AppendLock> Dataset::append_lock_dataset() const
{
    return std::make_shared<DatasetAppendLock>(*this);
}

std::shared_ptr<dataset::CheckLock> Dataset::check_lock_dataset() const
{
    return std::make_shared<DatasetCheckLock>(*this);
}

std::shared_ptr<dataset::ReadLock> Dataset::read_lock_segment(const std::string& relpath) const
{
    return std::make_shared<SegmentReadLock>(*this, relpath);
}

std::shared_ptr<dataset::AppendLock> Dataset::append_lock_segment(const std::string& relpath) const
{
    return std::make_shared<SegmentAppendLock>(*this, relpath);
}

std::shared_ptr<dataset::CheckLock> Dataset::check_lock_segment(const std::string& relpath) const
{
    return std::make_shared<SegmentCheckLock>(*this, relpath);
}


Reader::~Reader()
{
}

std::shared_ptr<dataset::Reader> Reader::archive()
{
    if (!m_archive)
        m_archive = dataset().archive()->create_reader();
    return m_archive;
}

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    if (!dataset().hasArchive()) return true;
    return archive()->query_data(q, dest);
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    if (dataset().hasArchive())
        archive()->query_summary(matcher, summary);
}

std::shared_ptr<core::cfg::Section> Reader::read_config(const std::string& path)
{
    // Read the config file inside the directory
    string name = str::basename(path);
    string file = str::joinpath(path, "config");

    File in(file, O_RDONLY);
    // Parse the config file into a new section
    auto res = core::cfg::Section::parse(in);
    // Fill in missing bits
    res->set("name", name);
    res->set("path", sys::abspath(path));
    return res;
}

std::shared_ptr<core::cfg::Sections> Reader::read_configs(const std::string& path)
{
    // Read the config file inside the directory
    string name = str::basename(path);
    string file = str::joinpath(path, "config");

    File in(file, O_RDONLY);
    // Parse the config file into a new section
    auto sec = core::cfg::Section::parse(in);
    // Fill in missing bits
    sec->set("name", name);
    sec->set("path", sys::abspath(path));

    // Return a Sections with only this section
    auto res = std::make_shared<core::cfg::Sections>();
    res->emplace(name, sec);
    return res;
}


Writer::~Writer()
{
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch)
{
    return segmented::Writer::test_acquire(session, cfg, batch);
}


Checker::~Checker()
{
}

std::shared_ptr<dataset::Checker> Checker::archive()
{
    if (!m_archive)
        m_archive = dataset().archive()->create_checker();
    return m_archive;
}

void Checker::repack(CheckerConfig& opts, unsigned test_flags)
{
    if (opts.offline && dataset().hasArchive())
        archive()->repack(opts, test_flags);
}

void Checker::check(CheckerConfig& opts)
{
    if (opts.offline && dataset().hasArchive())
        archive()->check(opts);
}

void Checker::remove_old(CheckerConfig& opts)
{
    if (opts.offline && dataset().hasArchive())
        archive()->remove_old(opts);
}

void Checker::remove_all(CheckerConfig& opts)
{
    if (opts.offline && dataset().hasArchive())
        archive()->remove_all(opts);
}

void Checker::tar(CheckerConfig& opts)
{
    if (opts.offline && dataset().hasArchive())
        archive()->tar(opts);
}

void Checker::zip(CheckerConfig& opts)
{
    if (opts.offline && dataset().hasArchive())
        archive()->zip(opts);
}

void Checker::compress(CheckerConfig& opts, unsigned groupsize)
{
    if (opts.offline && dataset().hasArchive())
        archive()->compress(opts, groupsize);
}

void Checker::check_issue51(CheckerConfig& opts)
{
    if (opts.offline && dataset().hasArchive())
        archive()->check_issue51(opts);
}

void Checker::state(CheckerConfig& opts)
{
    if (opts.offline && dataset().hasArchive())
        archive()->state(opts);
}

template class Base<Reader>;
template class Base<Checker>;

}
}
}
