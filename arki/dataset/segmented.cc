#include "segmented.h"
#include "step.h"
#include "ondisk2/writer.h"
#include "simple/writer.h"
#include "maintenance.h"
#include "archive.h"
#include "reporter.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan/any.h"
#include "arki/types/reftime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include <system_error>
#include <sstream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

SegmentedConfig::SegmentedConfig(const ConfigFile& cfg)
    : LocalConfig(cfg),
      step_name(str::lower(cfg.value("step"))),
      force_dir_segments(cfg.value("segments") == "dir"),
      mock_data(cfg.value("mockdata") == "true")
{
    if (step_name.empty())
        throw std::runtime_error("Dataset " + name + " misses step= configuration");

    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        default_replace_strategy = Writer::REPLACE_ALWAYS;
    else if (repl == "USN")
        default_replace_strategy = Writer::REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        default_replace_strategy = Writer::REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in the configuration of dataset " + name);

    std::string shard = cfg.value("shard");
    m_step = Step::create(step_name);
}

SegmentedConfig::~SegmentedConfig()
{
}

void SegmentedConfig::to_shard(const std::string& shard_path, std::shared_ptr<Step> step)
{
    LocalConfig::to_shard(shard_path);
    m_step = step;
}

std::unique_ptr<segment::SegmentManager> SegmentedConfig::create_segment_manager() const
{
    return segment::SegmentManager::get(path, force_dir_segments, mock_data);
}

std::shared_ptr<const SegmentedConfig> SegmentedConfig::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const SegmentedConfig>(new SegmentedConfig(cfg));
}


SegmentedReader::~SegmentedReader()
{
    delete m_segment_manager;
}

segment::SegmentManager& SegmentedReader::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}


SegmentedWriter::~SegmentedWriter()
{
    delete m_segment_manager;
}

segment::SegmentManager& SegmentedWriter::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}

Segment* SegmentedWriter::file(const Metadata& md, const std::string& format)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    string relname = config().step()(time) + "." + md.source().format;
    return segment_manager().get_segment(format, relname);
}

void SegmentedWriter::flush()
{
    segment_manager().flush_writers();
}

LocalWriter::AcquireResult SegmentedWriter::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

    if (type == "ondisk2" || type == "test")
        return dataset::ondisk2::Writer::testAcquire(cfg, md, out);
    if (type == "simple" || type == "error" || type == "duplicates")
        return dataset::simple::Writer::testAcquire(cfg, md, out);

    throw std::runtime_error("cannot simulate dataset acquisition: unknown dataset type \""+type+"\"");
}


SegmentedChecker::~SegmentedChecker()
{
    delete m_segment_manager;
}

segment::SegmentManager& SegmentedChecker::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}

void SegmentedChecker::archiveSegment(const std::string& relpath)
{
    // TODO: this is a hack to ensure that 'last' is created (and clean) before we start moving files into it.
    archive();

    const string& root = config().path;
    string pathname = str::joinpath(root, relpath);
    string arcrelname = str::joinpath("last", relpath);
    string arcabsname = str::joinpath(root, ".archive", arcrelname);
    sys::makedirs(str::dirname(arcabsname));

    // Sanity checks: avoid conflicts
    if (sys::exists(arcabsname))
    {
        stringstream ss;
        ss << "cannot archive " << pathname << " to " << arcabsname << " because the destination already exists";
        throw runtime_error(ss.str());
    }
    string src = pathname;
    string dst = arcabsname;
    bool compressed = scan::isCompressed(pathname);
    if (compressed)
    {
        src += ".gz";
        dst += ".gz";
        if (sys::exists(dst))
        {
            stringstream ss;
            ss << "cannot archive " << src << " to " << dst << " because the destination already exists";
            throw runtime_error(ss.str());
        }
    }

    // Remove stale metadata and summaries that may have been left around
    sys::unlink_ifexists(arcabsname + ".metadata");
    sys::unlink_ifexists(arcabsname + ".summary");

    // Move data to archive
    if (rename(src.c_str(), dst.c_str()) < 0)
    {
        stringstream ss;
        ss << "cannot rename " << src << " to " << dst;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
    if (compressed)
        sys::rename_ifexists(pathname + ".gz.idx", arcabsname + ".gz.idx");

    // Move metadata to archive
    sys::rename_ifexists(pathname + ".metadata", arcabsname + ".metadata");
    sys::rename_ifexists(pathname + ".summary", arcabsname + ".summary");

    // Acquire in the achive
    metadata::Collection mdc;
    if (sys::exists(arcabsname + ".metadata"))
        mdc.read_from_file(arcabsname + ".metadata");
    else if (compressed) {
        utils::compress::TempUnzip tu(arcabsname);
        scan::scan(arcabsname, mdc.inserter_func());
    } else
        scan::scan(arcabsname, mdc.inserter_func());

    archive().indexSegment(arcrelname, move(mdc));
}

size_t SegmentedChecker::removeSegment(const std::string& relpath, bool withData)
{
    if (withData)
        return segment_manager().remove(relpath);
    else
        return 0;
}

void SegmentedChecker::maintenance(dataset::Reporter& reporter, segment::state_func v, bool quick)
{
}

void SegmentedChecker::removeAll(dataset::Reporter& reporter, bool writable)
{
    // TODO: decide if we're removing archives at all
    // TODO: if (hasArchive())
    // TODO:    archive().removeAll(reporter, writable);
}

void SegmentedChecker::repack(dataset::Reporter& reporter, bool writable)
{
    const string& root = config().path;

    if (files::hasDontpackFlagfile(root))
    {
        reporter.operation_aborted(name(), "repack", "dataset needs checking first");
        return;
    }

    unique_ptr<maintenance::Agent> repacker;

    if (writable)
        // No safeguard against a deleted index: we catch that in the
        // constructor and create the don't pack flagfile
        repacker.reset(new maintenance::RealRepacker(reporter, *this));
    else
        repacker.reset(new maintenance::MockRepacker(reporter, *this));
    try {
        maintenance(reporter, [&](const std::string& relpath, segment::State state) {
            (*repacker)(relpath, state);
        });
        repacker->end();
    } catch (...) {
        files::createDontpackFlagfile(root);
        throw;
    }

    LocalChecker::repack(reporter, writable);
}

void SegmentedChecker::check(dataset::Reporter& reporter, bool fix, bool quick)
{
    const string& root = config().path;

    if (fix)
    {
        maintenance::RealFixer fixer(reporter, *this);
        try {
            maintenance(reporter, [&](const std::string& relpath, segment::State state) {
                fixer(relpath, state);
            }, quick);
            fixer.end();
        } catch (...) {
            files::createDontpackFlagfile(root);
            throw;
        }

        files::removeDontpackFlagfile(root);
    } else {
        maintenance::MockFixer fixer(reporter, *this);
        maintenance(reporter, [&](const std::string& relpath, segment::State state) {
            fixer(relpath, state);
        }, quick);
        fixer.end();
    }

    LocalChecker::check(reporter, fix, quick);
}

}
}
