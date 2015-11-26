#include <arki/dataset/local.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/simple/writer.h>
#include <arki/dataset/archive.h>
#include <arki/dataset/maintenance.h>
#include <arki/dataset/targetfile.h>
#include <arki/dataset/data.h>
#include <arki/utils/files.h>
#include <arki/configfile.h>
#include <arki/scan/any.h>
#include <arki/runtime/io.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/collection.h>
#include <arki/nag.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <iostream>
#include <fstream>
#include <sys/stat.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

Local::Local(const ConfigFile& cfg)
    : m_name(cfg.value("name")),
      m_path(cfg.value("path")),
      m_archive(0)
{
}

Local::~Local()
{
    if (m_archive) delete m_archive;
}

bool Local::hasArchive() const
{
    string arcdir = str::joinpath(m_path, ".archive");
    return sys::exists(arcdir);
}

Archives& Local::archive()
{
	if (!m_archive)
		m_archive = new Archives(m_path, str::joinpath(m_path, ".archive"));
	return *m_archive;
}

const Archives& Local::archive() const
{
	if (!m_archive)
		m_archive = new Archives(m_path, str::joinpath(m_path, ".archive"));
	return *m_archive;
}

void Local::queryData(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	if (hasArchive())
		archive().queryData(q, consumer);
}

void Local::querySummary(const Matcher& matcher, Summary& summary)
{
	if (hasArchive())
		archive().querySummary(matcher, summary);
}

size_t Local::produce_nth(metadata::Eater& cons, size_t idx)
{
    if (hasArchive())
        return archive().produce_nth(cons, idx);
    return 0;
}

namespace {
struct ScanTestFilter : public metadata::Eater
{
    const Matcher& filter;
    metadata::Eater& next;

    ScanTestFilter(const Matcher& filter, metadata::Eater& next)
        : filter(filter), next(next) {}

    bool eat(unique_ptr<Metadata>&& md) override
    {
        metadata::Collection c;

        // Inner scope to run cleanups before we produce anything
        {
            // Get the data
            const auto& data = md->getData();

            // Write the raw data to a temp file
            runtime::Tempfile tf;
            tf.stream().write((const char*)data.data(), data.size());
            if (tf.stream().bad())
            {
                stringstream ss;
                ss << "cannot write " << data.size() << " bytes to " << tf.name();
                throw std::system_error(errno, std::system_category(), ss.str());
            }
            tf.stream().flush();

            // Rescan the data
            try {
                scan::scan(tf.name(), c, md->source().format);
            } catch (std::exception& e) {
                // If scanning now fails, clear c so later we output the offender
                stringstream sstream;
                sstream << md->source();
                nag::verbose("%s: scanning failed: %s", sstream.str().c_str(), e.what());
                c.clear();
            }
        }

        // Check that collection has 1 element (not 0, not >1)
        if (c.size() != 1)
            return next.eat(move(md));

        // Match on the rescanned, if it fails, output it
        if (!filter(c[0]))
        {
            stringstream sstream;
            sstream << md->source();
            nag::verbose("%s: does not match filter");
            return next.eat(move(md));
        }

        // All fine, ready for the next one
        return true;
    }
};
}

size_t Local::scan_test(metadata::Eater& cons, size_t idx)
{
    std::map<std::string, std::string>::const_iterator i = cfg.find("filter");
    // No point in running a scan_test if there is no filter
    if (i == cfg.end())
        return 0;
    // Dataset filter that we use to validate produce_nth output
    Matcher filter = Matcher::parse(i->second);
    // Filter keeping only those data that, once rescanned, DO NOT match
    ScanTestFilter f(filter, cons);
    // Produce samples to be checked
    return produce_nth(f, idx);
}

void Local::readConfig(const std::string& path, ConfigFile& cfg)
{
	if (path == "-")
	{
		// Parse the config file from stdin
		cfg.parse(cin, path);
		return;
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

        ConfigFile section;
        ifstream in;
        in.open(file.c_str(), ios::in);
        if (!in.is_open() || in.fail())
        {
            stringstream ss;
            ss << "cannot open " << file << " for reading";
            throw std::system_error(errno, std::system_category(), ss.str());
        }
        // Parse the config file into a new section
        section.parse(in, file);
        // Fill in missing bits
        section.setValue("name", name);
        section.setValue("path", sys::abspath(fname));
        // Merge into cfg
        cfg.mergeInto(name, section);
    } else {
        // If it's a file, then it's a merged config file
        ifstream in;
        in.open(fname.c_str(), ios::in);
        if (!in.is_open() || in.fail())
        {
            stringstream ss;
            ss << "cannot open config file " << fname << " for reading";
            throw std::system_error(errno, std::system_category(), ss.str());
        }
        // Parse the config file
        cfg.parse(in, fname);
    }
}

SegmentedLocal::SegmentedLocal(const ConfigFile& cfg)
    : Local(cfg), m_segment_manager(data::SegmentManager::get(cfg).release())
{
}

SegmentedLocal::~SegmentedLocal()
{
    if (m_segment_manager) delete m_segment_manager;
}

WritableLocal::WritableLocal(const ConfigFile& cfg)
    : m_path(cfg.value("path")),
      m_archive(0), m_archive_age(-1), m_delete_age(-1),
      m_default_replace_strategy(REPLACE_NEVER),
      m_tf(TargetFile::create(cfg)),
      m_segment_manager(data::SegmentManager::get(cfg).release())
{
    m_name = cfg.value("name");
    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        m_default_replace_strategy = REPLACE_ALWAYS;
    else if (repl == "USN")
        m_default_replace_strategy = REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        m_default_replace_strategy = REPLACE_NEVER;
    else
        throw wibble::exception::Consistency("reading configuration for dataset " + m_name, "Replace strategy '" + repl + "' is not recognised");

	string tmp = cfg.value("archive age");
	if (!tmp.empty())
		m_archive_age = strtoul(tmp.c_str(), 0, 10);
	tmp = cfg.value("delete age");
	if (!tmp.empty())
		m_delete_age = strtoul(tmp.c_str(), 0, 10);
}

WritableLocal::~WritableLocal()
{
    if (m_segment_manager) delete m_segment_manager;
    if (m_tf) delete m_tf;
    if (m_archive) delete m_archive;
}

bool WritableLocal::hasArchive() const
{
    string arcdir = str::joinpath(m_path, ".archive");
    return sys::exists(arcdir);
}

Archives& WritableLocal::archive()
{
	if (!m_archive)
		m_archive = new Archives(m_path, str::joinpath(m_path, ".archive"), false);
	return *m_archive;
}

const Archives& WritableLocal::archive() const
{
	if (!m_archive)
		m_archive = new Archives(m_path, str::joinpath(m_path, ".archive"), false);
	return *m_archive;
}

data::Segment* WritableLocal::file(const Metadata& md, const std::string& format)
{
    string relname = (*m_tf)(md) + "." + md.source().format;
    return m_segment_manager->get_segment(format, relname);
}

void WritableLocal::flush()
{
    m_segment_manager->flush_writers();
}

void WritableLocal::archiveFile(const std::string& relpath)
{
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
	archive().acquire(arcrelname);
}

size_t WritableLocal::removeFile(const std::string& relpath, bool withData)
{
    if (withData)
        return m_segment_manager->remove(relpath);
    else
        return 0;
}

void WritableLocal::maintenance(maintenance::MaintFileVisitor& v, bool quick)
{
	if (hasArchive())
		archive().maintenance(v);
}

void WritableLocal::removeAll(std::ostream& log, bool writable)
{
	// TODO: decide if we're removing archives at all
	// TODO: if (hasArchive())
	// TODO: 	archive().removeAll(log, writable);
}

void WritableLocal::sanityChecks(std::ostream& log, bool writable)
{
}

void WritableLocal::repack(std::ostream& log, bool writable)
{
	if (files::hasDontpackFlagfile(m_path))
	{
		log << m_path << ": dataset needs checking first" << endl;
		return;
	}

	unique_ptr<maintenance::Agent> repacker;

	if (writable)
		// No safeguard against a deleted index: we catch that in the
		// constructor and create the don't pack flagfile
		repacker.reset(new maintenance::RealRepacker(log, *this));
	else
		repacker.reset(new maintenance::MockRepacker(log, *this));
	try {
		maintenance(*repacker);
		repacker->end();
	} catch (...) {
		files::createDontpackFlagfile(m_path);
		throw;
	}
}

void WritableLocal::check(std::ostream& log, bool fix, bool quick)
{
	if (fix)
	{
		maintenance::RealFixer fixer(log, *this);
		try {
			maintenance(fixer, quick);
			fixer.end();
		} catch (...) {
			files::createDontpackFlagfile(m_path);
			throw;
		}

		files::removeDontpackFlagfile(m_path);
	} else {
		maintenance::MockFixer fixer(log, *this);
		maintenance(fixer, quick);
		fixer.end();
	}

	sanityChecks(log, fix);
}

WritableLocal* WritableLocal::create(const ConfigFile& cfg)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

	if (type == "ondisk2" || type == "test")
		return new dataset::ondisk2::Writer(cfg);
	if (type == "simple" || type == "error" || type == "duplicates")
		return new dataset::simple::Writer(cfg);

	throw wibble::exception::Consistency("creating a dataset", "unknown dataset type \""+type+"\"");
}

WritableLocal::AcquireResult WritableLocal::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

	if (type == "ondisk2" || type == "test")
		return dataset::ondisk2::Writer::testAcquire(cfg, md, out);
	if (type == "simple" || type == "error" || type == "duplicates")
		return dataset::simple::Writer::testAcquire(cfg, md, out);

	throw wibble::exception::Consistency("simulating dataset acquisition", "unknown dataset type \""+type+"\"");
}


}
}
// vim:set ts=4 sw=4:
