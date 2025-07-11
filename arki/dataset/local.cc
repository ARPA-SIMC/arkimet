#include "local.h"
#include "archive.h"
#include "arki/dataset/time.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "segmented.h"
#include <fcntl.h>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace local {

Dataset::Dataset(std::shared_ptr<Session> session,
                 const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg),
      path(sys::abspath(std::filesystem::path(cfg.value("path"))))
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

std::pair<bool, metadata::Inbound::Result>
Dataset::check_acquire_age(Metadata& md) const
{
    const auto& st                     = SessionTime::get();
    const types::reftime::Position* rt = md.get<types::reftime::Position>();
    auto time                          = rt->get_Position();

    if (delete_age != -1 && time < st.age_threshold(delete_age))
    {
        md.add_note("Safely discarded: data is older than delete age");
        return make_pair(true, metadata::Inbound::Result::OK);
    }

    if (archive_age != -1 && time < st.age_threshold(archive_age))
    {
        md.add_note("Import refused: data is older than archive age");
        return make_pair(true, metadata::Inbound::Result::ERROR);
    }

    return make_pair(false, metadata::Inbound::Result::OK);
}

std::shared_ptr<archive::Dataset> Dataset::archive()
{
    throw std::runtime_error("dataset does not support archives");
}

bool Dataset::hasArchive() const
{
    return std::filesystem::exists(path / ".archive");
}

Reader::~Reader() {}

std::shared_ptr<dataset::Reader> Reader::archive()
{
    if (!m_archive)
        m_archive = dataset().archive()->create_reader();
    return m_archive;
}

bool Reader::impl_query_data(const query::Data& q, metadata_dest_func dest)
{
    if (!dataset().hasArchive())
        return true;
    return archive()->query_data(q, dest);
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    if (dataset().hasArchive())
        archive()->query_summary(matcher, summary);
}

std::shared_ptr<core::cfg::Section>
Reader::read_config(const std::filesystem::path& path_)
{
    // Read the config file inside the directory
    auto path = sys::abspath(path_);
    auto name = path.filename();
    auto file = path / "config";

    if (std::filesystem::exists(file))
    {
        File in(file, O_RDONLY);
        // Parse the config file into a new section
        auto res = core::cfg::Section::parse(in);
        // Fill in missing bits
        res->set("name", name);

        if (res->value("type") != "remote")
            res->set("path", sys::abspath(path));
        return res;
    }
    else
    {
        auto abspath = std::filesystem::canonical(path);
        if (abspath.parent_path().filename() == ".archive")
        {
            auto containing_path = abspath.parent_path().parent_path();
            file                 = containing_path / "config";
            if (std::filesystem::exists(file))
            {
                File in(file, O_RDONLY);
                // Parse the config file into a new section
                auto res = core::cfg::Section::parse(in);
                // Fill in missing bits
                res->set("name", name);
                res->set("type", "simple");
                res->set("path", abspath);
                res->unset("archive age");
                res->unset("delete age");
                return res;
            }
            else
            {
                throw std::runtime_error(
                    path.native() +
                    ": path looks like an archive component but containing "
                    "configuration not found at " +
                    file.native());
            }
        }
        else
        {
            throw std::runtime_error(
                path.native() +
                ": path is a directory but dataset configuration not found");
        }
    }
}

std::shared_ptr<core::cfg::Sections>
Reader::read_configs(const std::filesystem::path& path)
{
    auto sec = read_config(path);
    auto res = std::make_shared<core::cfg::Sections>();
    res->emplace(sec->value("name"), sec);
    return res;
}

Writer::~Writer() {}

void Writer::test_acquire(std::shared_ptr<Session> session,
                          const core::cfg::Section& cfg,
                          metadata::InboundBatch& batch)
{
    return segmented::Writer::test_acquire(session, cfg, batch);
}

Checker::~Checker() {}

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

} // namespace local
} // namespace dataset
} // namespace arki
