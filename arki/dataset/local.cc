/*
 * dataset/local - Base class for local datasets
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

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
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <iostream>
#include <fstream>
#include <sys/stat.h>

using namespace std;
using namespace wibble;
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
	return sys::fs::exists(arcdir);
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

void Local::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	if (hasArchive())
		archive().queryData(q, consumer);
}

void Local::querySummary(const Matcher& matcher, Summary& summary)
{
	if (hasArchive())
		archive().querySummary(matcher, summary);
}

size_t Local::produce_nth(metadata::Consumer& cons, size_t idx)
{
    if (hasArchive())
        return archive().produce_nth(cons, idx);
    return 0;
}

namespace {
struct ScanTestFilter : public metadata::Consumer
{
    const Matcher& filter;
    metadata::Consumer& next;

    ScanTestFilter(const Matcher& filter, metadata::Consumer& next)
        : filter(filter), next(next) {}

    bool operator()(Metadata& md)
    {
        metadata::Collection c;

        // Inner scope to run cleanups before we produce anything
        {
            // Get the data
            sys::Buffer data = md.getData();

            // Write the raw data to a temp file
            runtime::Tempfile tf;
            tf.stream().write((const char*)data.data(), data.size());
            if (tf.stream().bad())
                throw wibble::exception::File(tf.name(), str::fmtf("cannot write %zd bytes to the file", data.size()));
            tf.stream().flush();

            // Rescan the data
            try {
                scan::scan(tf.name(), c, md.source().format);
            } catch (std::exception& e) {
                // If scanning now fails, clear c so later we output the offender
                stringstream sstream;
                sstream << md.source();
                nag::verbose("%s: scanning failed: %s", sstream.str().c_str(), e.what());
                c.clear();
            }
        }

        // Check that collection has 1 element (not 0, not >1)
        if (c.size() != 1)
            return next(md);

        // Match on the rescanned, if it fails, output it
        if (!filter(c[0]))
        {
            stringstream sstream;
            sstream << md.source();
            nag::verbose("%s: does not match filter");
            return next(md);
        }

        // All fine, ready for the next one
        return true;
    }
};
}

size_t Local::scan_test(metadata::Consumer& cons, size_t idx)
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
	using namespace wibble;

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
	std::auto_ptr<struct stat> st = sys::fs::stat(fname);
	if (st.get() == 0)
		throw wibble::exception::Consistency("reading configuration from " + fname, fname + " does not exist");
	if (S_ISDIR(st->st_mode))
	{
		// If it's a directory, merge in its config file
		string name = str::basename(fname);
		string file = str::joinpath(fname, "config");

		ConfigFile section;
		ifstream in;
		in.open(file.c_str(), ios::in);
		if (!in.is_open() || in.fail())
			throw wibble::exception::File(file, "opening config file for reading");
		// Parse the config file into a new section
		section.parse(in, file);
		// Fill in missing bits
		section.setValue("name", name);
		section.setValue("path", sys::fs::abspath(fname));
		// Merge into cfg
		cfg.mergeInto(name, section);
	} else {
		// If it's a file, then it's a merged config file
		ifstream in;
		in.open(fname.c_str(), ios::in);
		if (!in.is_open() || in.fail())
			throw wibble::exception::File(fname, "opening config file for reading");
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
	return sys::fs::exists(arcdir);
	//std::auto_ptr<struct stat> st = sys::fs::stat(arcdir);
	//if (!st.get())
		//return false;
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

data::Writer* WritableLocal::file(const Metadata& md, const std::string& format)
{
    string relname = (*m_tf)(md) + "." + md.source().format;
    return m_segment_manager->get_writer(format, relname);
}

void WritableLocal::flush()
{
    m_segment_manager->flush_writers();
}

void WritableLocal::archiveFile(const std::string& relpath)
{
	string pathname = str::joinpath(m_path, relpath);
	string arcrelname = str::joinpath("last", relpath);
	string arcabsname = str::joinpath(m_path, str::joinpath(".archive", arcrelname));
	sys::fs::mkFilePath(arcabsname);
	
	// Sanity checks: avoid conflicts
	if (sys::fs::exists(arcabsname))
		throw wibble::exception::Consistency("archiving " + pathname + " to " + arcabsname,
				arcabsname + " already exists");
	string src = pathname;
	string dst = arcabsname;
	bool compressed = scan::isCompressed(pathname);
	if (compressed)
	{
		src += ".gz";
		dst += ".gz";
		if (sys::fs::exists(dst))
			throw wibble::exception::Consistency("archiving " + src + " to " + dst,
					dst + " already exists");
	}

	// Remove stale metadata and summaries that may have been left around
	sys::fs::deleteIfExists(arcabsname + ".metadata");
	sys::fs::deleteIfExists(arcabsname + ".summary");

	// Move data to archive
	if (rename(src.c_str(), dst.c_str()) < 0)
		throw wibble::exception::System("moving " + src + " to " + dst);
	if (compressed)
		sys::fs::renameIfExists(pathname + ".gz.idx", arcabsname + ".gz.idx");

	// Move metadata to archive
    sys::fs::renameIfExists(pathname + ".metadata", arcabsname + ".metadata");
    sys::fs::renameIfExists(pathname + ".summary", arcabsname + ".summary");

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

	auto_ptr<maintenance::Agent> repacker;

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
	string type = wibble::str::tolower(cfg.value("type"));
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
	string type = wibble::str::tolower(cfg.value("type"));
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
