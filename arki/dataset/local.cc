/*
 * dataset/local - Base class for local datasets
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/local.h>
#include <arki/dataset/archive.h>
#include <arki/dataset/maintenance.h>
#include <arki/utils/files.h>
#include <arki/configfile.h>
#include <arki/scan/any.h>
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
	: m_name(cfg.value("name")), m_path(cfg.value("path")), m_archive(0)
{
}

Local::~Local()
{
	if (m_archive) delete m_archive;
}

bool Local::hasArchive() const
{
	string arcdir = str::joinpath(m_path, ".archive");
	return sys::fs::access(arcdir, F_OK);
}

Archives& Local::archive()
{
	if (!m_archive)
		m_archive = new Archives(str::joinpath(m_path, ".archive"));
	return *m_archive;
}

const Archives& Local::archive() const
{
	if (!m_archive)
		m_archive = new Archives(str::joinpath(m_path, ".archive"));
	return *m_archive;
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

WritableLocal::WritableLocal(const ConfigFile& cfg)
	: m_path(cfg.value("path")), m_archive(0),
	  m_archive_age(-1), m_delete_age(-1)
{
	string tmp = cfg.value("archive age");
	if (!tmp.empty())
		m_archive_age = strtoul(tmp.c_str(), 0, 10);
	tmp = cfg.value("delete age");
	if (!tmp.empty())
		m_delete_age = strtoul(tmp.c_str(), 0, 10);
}

WritableLocal::~WritableLocal()
{
	if (m_archive) delete m_archive;
}

bool WritableLocal::hasArchive() const
{
	string arcdir = str::joinpath(m_path, ".archive");
	return sys::fs::access(arcdir, F_OK);
	//std::auto_ptr<struct stat> st = sys::fs::stat(arcdir);
	//if (!st.get())
		//return false;
}

Archives& WritableLocal::archive()
{
	if (!m_archive)
		m_archive = new Archives(str::joinpath(m_path, ".archive"), false);
	return *m_archive;
}

const Archives& WritableLocal::archive() const
{
	if (!m_archive)
		m_archive = new Archives(str::joinpath(m_path, ".archive"), false);
	return *m_archive;
}

void WritableLocal::archiveFile(const std::string& relpath)
{
	string pathname = str::joinpath(m_path, relpath);
	string arcrelname = str::joinpath("last", relpath);
	string arcabsname = str::joinpath(m_path, str::joinpath(".archive", arcrelname));
	sys::fs::mkFilePath(arcabsname);
	
	// Sanity checks: avoid conflicts
	if (sys::fs::access(arcabsname, F_OK))
		throw wibble::exception::Consistency("archiving " + pathname + " to " + arcabsname,
				arcabsname + " already exists");
	string src = pathname;
	string dst = arcabsname;
	if (scan::isCompressed(pathname))
	{
		src += ".gz";
		dst += ".gz";
		if (sys::fs::access(dst, F_OK))
			throw wibble::exception::Consistency("archiving " + src + " to " + dst,
					dst + " already exists");
	}

	// Remove stale metadata and summaries that may have been left around
	sys::fs::deleteIfExists(arcabsname + ".metadata");
	sys::fs::deleteIfExists(arcabsname + ".summary");

	// Move data to archive
	if (rename(src.c_str(), dst.c_str()) < 0)
		throw wibble::exception::System("moving " + src + " to " + dst);

	// Move metadata to archive
	files::renameIfExists(pathname + ".metadata", arcabsname + ".metadata");
	files::renameIfExists(pathname + ".summary", arcabsname + ".summary");

	// Acquire in the achive
	archive().acquire(arcrelname);
}

void WritableLocal::maintenance(maintenance::MaintFileVisitor& v, bool quick)
{
	if (hasArchive())
		archive().maintenance(v);
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
}


}
}
// vim:set ts=4 sw=4:
