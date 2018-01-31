#include "segmented.h"
#include "step.h"
#include "time.h"
#include "ondisk2/writer.h"
#include "simple/writer.h"
#include "iseg/writer.h"
#include "maintenance.h"
#include "archive.h"
#include "reporter.h"
#include "arki/dataset/lock.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan/any.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include "arki/configfile.h"
#include <system_error>
#include <sstream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segmented {

bool State::has(const std::string& relpath) const
{
    return find(relpath) != end();
}

const SegmentState& State::get(const std::string& seg) const
{
    auto res = find(seg);
    if (res == end())
        throw std::runtime_error("segment " + seg + " not found in state");
    return res->second;
}

void SegmentState::check_age(const std::string& relpath, const Config& cfg, dataset::Reporter& reporter)
{
    core::Time archive_threshold(0, 0, 0);
    core::Time delete_threshold(0, 0, 0);
    const auto& st = SessionTime::get();

    if (cfg.archive_age != -1)
        archive_threshold = st.age_threshold(cfg.archive_age);
    if (cfg.delete_age != -1)
        delete_threshold = st.age_threshold(cfg.delete_age);

    if (delete_threshold.ye != 0 && delete_threshold >= until)
    {
        reporter.segment_info(cfg.name, relpath, "segment old enough to be deleted");
        state = state + SEGMENT_DELETE_AGE;
        return;
    }

    if (archive_threshold.ye != 0 && archive_threshold >= until)
    {
        reporter.segment_info(cfg.name, relpath, "segment old enough to be archived");
        state = state + SEGMENT_ARCHIVE_AGE;
        return;
    }
}

unsigned State::count(segment::State state) const
{
    unsigned res = 0;
    for (const auto& i: *this)
        if (i.second.state == state)
            ++res;
    return res;
}

void State::dump(FILE* out) const
{
    for (const auto& i: *this)
        fprintf(stderr, "%s: %s %s to %s\n", i.first.c_str(), i.second.state.to_string().c_str(), i.second.begin.to_iso8601(' ').c_str(), i.second.until.to_iso8601(' ').c_str());
}

Config::Config(const ConfigFile& cfg)
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

Config::~Config()
{
}

bool Config::relpath_timespan(const std::string& path, core::Time& start_time, core::Time& end_time) const
{
    return step().path_timespan(path, start_time, end_time);
}

std::unique_ptr<segment::Manager> Config::create_segment_manager() const
{
    return segment::Manager::get(path, force_dir_segments, mock_data);
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}


Reader::~Reader()
{
    delete m_segment_manager;
}

segment::Manager& Reader::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}


Writer::~Writer()
{
    delete m_segment_manager;
}

segment::Manager& Writer::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}

std::shared_ptr<segment::Writer> Writer::file(const Metadata& md, const std::string& format)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    string relname = config().step()(time) + "." + md.source().format;
    return segment_manager().get_writer(format, relname);
}

void Writer::flush()
{
}

void Writer::test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

    if (type == "iseg")
        return dataset::iseg::Writer::test_acquire(cfg, batch, out);
    if (type == "ondisk2" || type == "test")
        return dataset::ondisk2::Writer::test_acquire(cfg, batch, out);
    if (type == "simple" || type == "error" || type == "duplicates")
        return dataset::simple::Writer::test_acquire(cfg, batch, out);

    throw std::runtime_error("cannot simulate dataset acquisition: unknown dataset type \""+type+"\"");
}

CheckerSegment::CheckerSegment(std::shared_ptr<dataset::CheckLock> lock)
    : lock(lock)
{
}

CheckerSegment::~CheckerSegment()
{
}

void CheckerSegment::release(const std::string& destpath)
{
    sys::makedirs(str::dirname(destpath));

    // Sanity checks: avoid conflicts
    if (sys::exists(destpath))
    {
        stringstream ss;
        ss << "cannot archive " << segment->absname << " to " << destpath << " because the destination already exists";
        throw runtime_error(ss.str());
    }
    string src = segment->absname;
    string dst = destpath;
    bool compressed = scan::isCompressed(src);
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
    sys::unlink_ifexists(destpath + ".metadata");
    sys::unlink_ifexists(destpath + ".summary");

    // Move data to archive
    if (rename(src.c_str(), dst.c_str()) < 0)
    {
        stringstream ss;
        ss << "cannot rename " << src << " to " << dst;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
    if (compressed)
        sys::rename_ifexists(src + ".idx", dst + ".idx");

    // Move metadata to archive
    sys::rename_ifexists(segment->absname + ".metadata", destpath + ".metadata");
    sys::rename_ifexists(segment->absname + ".summary", destpath + ".summary");
}

void CheckerSegment::archive()
{
    // TODO: this is a hack to ensure that 'last' is created (and clean) before
    // we start moving files into it. The "created" part is not a problem:
    // releaseSegment will create all relevant paths. The "clean" part is the
    // problem, because opening a writer on an already existing path creates a
    // needs-check-do-not-pack file
    archives();

    // Get the format for this relpath
    size_t pos = segment->relname.rfind(".");
    if (pos == string::npos)
        throw std::runtime_error("cannot archive segment " + segment->absname + " because it does not have a format extension");
    string format = segment->relname.substr(pos + 1);

    // Get the time range for this relpath
    core::Time start_time, end_time;
    if (!config().relpath_timespan(segment->relname, start_time, end_time))
        throw std::runtime_error("cannot archive segment " + segment->absname + " because its name does not match the dataset step");

    // Get the archive relpath for this segment
    string arcrelpath = str::joinpath("last", config().step()(start_time)) + "." + format;
    string arcabspath = str::joinpath(config().path, ".archive", arcrelpath);
    release(arcabspath);

    bool compressed = scan::isCompressed(arcabspath);

    // Acquire in the achive
    metadata::Collection mdc;
    if (sys::exists(arcabspath + ".metadata"))
        mdc.read_from_file(arcabspath + ".metadata");
    else if (compressed) {
        utils::compress::TempUnzip tu(arcabspath);
        scan::scan(arcabspath, lock, mdc.inserter_func());
    } else
        scan::scan(arcabspath, lock, mdc.inserter_func());

    archives().indexSegment(arcrelpath, move(mdc));
}

void CheckerSegment::unarchive()
{
    string arcrelpath = str::joinpath("last", segment->relname);
    archives().releaseSegment(arcrelpath, segment->absname);

    bool compressed = scan::isCompressed(segment->absname);

    // Acquire in the achive
    metadata::Collection mdc;
    if (sys::exists(segment->absname + ".metadata"))
        mdc.read_from_file(segment->absname + ".metadata");
    else if (compressed) {
        utils::compress::TempUnzip tu(segment->absname);
        scan::scan(segment->absname, lock, mdc.inserter_func());
    } else
        scan::scan(segment->absname, lock, mdc.inserter_func());
    index(move(mdc));
}


Checker::~Checker()
{
    delete m_segment_manager;
}

segment::Manager& Checker::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}

void Checker::segments_all(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments([&](CheckerSegment& segment) { dest(segment); });
    segments_untracked([&](segmented::CheckerSegment& segment) { dest(segment); });
}

void Checker::segments_all_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments_filtered(matcher, [&](CheckerSegment& segment) { dest(segment); });
    segments_untracked_filtered(matcher, [&](segmented::CheckerSegment& segment) { dest(segment); });
}

segmented::State Checker::scan(dataset::Reporter& reporter, bool quick)
{
    segmented::State segments_state;
    segments_all([&](CheckerSegment& segment) {
        segments_state.insert(make_pair(segment.path_relative(), segment.scan(reporter, quick)));
    });
    return segments_state;
}

segmented::State Checker::scan_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool quick)
{
    segmented::State segments_state;
    segments_all_filtered(matcher, [&](CheckerSegment& segment) {
        segments_state.insert(make_pair(segment.path_relative(), segment.scan(reporter, quick)));
    });
    return segments_state;
}

void Checker::remove_all(dataset::Reporter& reporter, bool writable)
{
    // TODO: decide if we're removing archives at all
    // TODO: if (hasArchive())
    // TODO:    archive().removeAll(reporter, writable);
    segments_all([&](CheckerSegment& segment) {
        if (writable)
        {
            auto freed = segment.remove(true);
            reporter.segment_delete(name(), segment.path_relative(), "deleted (" + std::to_string(freed) + " freed)");
        }
        else
            reporter.segment_delete(name(), segment.path_relative(), "should be deleted");
    });
}

void Checker::remove_all_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable)
{
    // TODO: decide if we're removing archives at all
    // TODO: if (hasArchive())
    // TODO:    archive().removeAll(reporter, writable);
    segments_all_filtered(matcher, [&](CheckerSegment& segment) {
        if (writable)
        {
            auto freed = segment.remove(true);
            reporter.segment_delete(name(), segment.path_relative(), "deleted (" + std::to_string(freed) + " freed)");
        }
        else
            reporter.segment_delete(name(), segment.path_relative(), "should be deleted");
    });
}


void Checker::repack(dataset::Reporter& reporter, bool writable, unsigned test_flags)
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
        repacker.reset(new maintenance::RealRepacker(reporter, *this, test_flags));
    else
        repacker.reset(new maintenance::MockRepacker(reporter, *this, test_flags));

    try {
        segments_all([&](CheckerSegment& segment) {
            (*repacker)(segment, segment.scan(reporter, true).state);
        });
        repacker->end();
    } catch (...) {
        // FIXME: this only makes sense for ondisk2
        // FIXME: also for iseg. Add the marker at the segment level instead of
        // the dataset level, so that the try/catch can be around the single
        // segment, and cleared on a segment by segment basis
        files::createDontpackFlagfile(root);
        throw;
    }

    LocalChecker::repack(reporter, writable);
}

void Checker::repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable, unsigned test_flags)
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
        repacker.reset(new maintenance::RealRepacker(reporter, *this, test_flags));
    else
        repacker.reset(new maintenance::MockRepacker(reporter, *this, test_flags));

    try {
        segments_all_filtered(matcher, [&](CheckerSegment& segment) {
            (*repacker)(segment, segment.scan(reporter, true).state);
        });
        repacker->end();
    } catch (...) {
        // FIXME: this only makes sense for ondisk2
        // FIXME: also for iseg. Add the marker at the segment level instead of
        // the dataset level, so that the try/catch can be around the single
        // segment, and cleared on a segment by segment basis
        files::createDontpackFlagfile(root);
        throw;
    }

    LocalChecker::repack(reporter, writable);
}

void Checker::check(dataset::Reporter& reporter, bool fix, bool quick)
{
    const string& root = config().path;

    if (fix)
    {
        maintenance::RealFixer fixer(reporter, *this);
        try {
            segments_all([&](CheckerSegment& segment) {
                fixer(segment, segment.scan(reporter, quick).state);
            });
            fixer.end();
        } catch (...) {
            // FIXME: Add the marker at the segment level instead of
            // the dataset level, so that the try/catch can be around the single
            // segment, and cleared on a segment by segment basis
            files::createDontpackFlagfile(root);
            throw;
        }

        files::removeDontpackFlagfile(root);
    } else {
        maintenance::MockFixer fixer(reporter, *this);
        segments_all([&](CheckerSegment& segment) {
            fixer(segment, segment.scan(reporter, quick).state);
        });
        fixer.end();
    }

    LocalChecker::check(reporter, fix, quick);
}

void Checker::check_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool fix, bool quick)
{
    const string& root = config().path;

    if (fix)
    {
        maintenance::RealFixer fixer(reporter, *this);
        try {
            segments_all_filtered(matcher, [&](CheckerSegment& segment) {
                fixer(segment, segment.scan(reporter, quick).state);
            });
            fixer.end();
        } catch (...) {
            // FIXME: Add the marker at the segment level instead of
            // the dataset level, so that the try/catch can be around the single
            // segment, and cleared on a segment by segment basis
            files::createDontpackFlagfile(root);
            throw;
        }

        files::removeDontpackFlagfile(root);
    } else {
        maintenance::MockFixer fixer(reporter, *this);
        segments_all_filtered(matcher, [&](CheckerSegment& segment) {
            fixer(segment, segment.scan(reporter, quick).state);
        });
        fixer.end();
    }

    LocalChecker::check(reporter, fix, quick);
}

}
}
}
