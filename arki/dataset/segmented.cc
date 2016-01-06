#include "segmented.h"
#include "step.h"
#include "ondisk2.h"
#include "simple/writer.h"
#include "maintenance.h"
#include "archive.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan/any.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include <sstream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

SegmentedBase::SegmentedBase(const ConfigFile& cfg)
    : m_segment_manager(segment::SegmentManager::get(cfg).release())
{
}

SegmentedBase::~SegmentedBase()
{
    delete m_segment_manager;
}


SegmentedReader::SegmentedReader(const ConfigFile& cfg)
    : LocalReader(cfg), SegmentedBase(cfg)
{
}

SegmentedReader::~SegmentedReader()
{
}


SegmentedWriter::SegmentedWriter(const ConfigFile& cfg)
    : LocalWriter(cfg), SegmentedBase(cfg),
      m_default_replace_strategy(REPLACE_NEVER),
      m_step(Step::create(cfg))
{
    if (!m_step)
        throw std::runtime_error("cannot create a writer for dataset " + m_name + ": no step has been configured");

    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        m_default_replace_strategy = REPLACE_ALWAYS;
    else if (repl == "USN")
        m_default_replace_strategy = REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        m_default_replace_strategy = REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in configuration for dataset " + m_name);
}

SegmentedWriter::~SegmentedWriter()
{
    delete m_step;
}

Segment* SegmentedWriter::file(const Metadata& md, const std::string& format)
{
    string relname = (*m_step)(md) + "." + md.source().format;
    return m_segment_manager->get_segment(format, relname);
}

void SegmentedWriter::flush()
{
    m_segment_manager->flush_writers();
}

SegmentedWriter* SegmentedWriter::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

    if (type == "ondisk2" || type == "test")
        return new dataset::ondisk2::Writer(cfg);
    if (type == "simple" || type == "error" || type == "duplicates")
        return new dataset::simple::Writer(cfg);

    throw std::runtime_error("cannot create dataset: unknown dataset type \""+type+"\"");
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



SegmentedChecker::SegmentedChecker(const ConfigFile& cfg)
    : LocalChecker(cfg), SegmentedBase(cfg), m_step(Step::create(cfg))
{
}

SegmentedChecker::~SegmentedChecker()
{
    delete m_step;
}

void SegmentedChecker::archiveSegment(const std::string& relpath)
{
    // TODO: this is a hack to ensure that 'last' is created (and clean) before we start moving files into it.
    archive();

    string pathname = str::joinpath(m_path, relpath);
    string arcrelname = str::joinpath("last", relpath);
    string arcabsname = str::joinpath(m_path, ".archive", arcrelname);
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
        return m_segment_manager->remove(relpath);
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
    if (files::hasDontpackFlagfile(m_path))
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
        files::createDontpackFlagfile(m_path);
        throw;
    }

    LocalChecker::repack(reporter, writable);
}

void SegmentedChecker::check(dataset::Reporter& reporter, bool fix, bool quick)
{
    if (fix)
    {
        maintenance::RealFixer fixer(reporter, *this);
        try {
            maintenance(reporter, [&](const std::string& relpath, segment::State state) {
                fixer(relpath, state);
            }, quick);
            fixer.end();
        } catch (...) {
            files::createDontpackFlagfile(m_path);
            throw;
        }

        files::removeDontpackFlagfile(m_path);
    } else {
        maintenance::MockFixer fixer(reporter, *this);
        maintenance(reporter, [&](const std::string& relpath, segment::State state) {
            fixer(relpath, state);
        }, quick);
        fixer.end();
    }

    LocalChecker::check(reporter, fix, quick);
}

SegmentedChecker* SegmentedChecker::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

    if (type == "ondisk2" || type == "test")
        return new dataset::ondisk2::Checker(cfg);
    if (type == "simple" || type == "error" || type == "duplicates")
        return new dataset::simple::Checker(cfg);

    throw std::runtime_error("cannot create dataset: unknown dataset type \""+type+"\"");
}

}
}
