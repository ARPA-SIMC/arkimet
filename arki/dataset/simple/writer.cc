/*
 * dataset/simple/writer - Writer for simple datasets with no duplicate checks
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/simple/writer.h>
#include <arki/dataset/simple/index.h>
#include <arki/dataset/simple/datafile.h>
#include <arki/dataset/targetfile.h>
#include <arki/dataset/maintenance.h>
#include <arki/types/assigneddataset.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/metadata/collection.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/scan/any.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <fstream>
#include <ctime>
#include <cstdio>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Writer::Writer(const ConfigFile& cfg)
	: WritableLocal(cfg), m_mft(0), m_tf(0)
{
	m_name = cfg.value("name");

	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_path);

	m_tf = TargetFile::create(cfg);

	// If the index is missing, take note not to perform a repack until a
	// check is made
	if (!simple::Manifest::exists(m_path))
		files::createDontpackFlagfile(m_path);

	auto_ptr<simple::Manifest> mft = simple::Manifest::create(m_path);
	m_mft = mft.release();
	m_mft->openRW();
}

Writer::~Writer()
{
	flush();
	if (m_tf) delete m_tf;
	if (m_mft) delete m_mft;
}

std::string Writer::id(const Metadata& md) const
{
	return std::string();
}

Datafile* Writer::file(const std::string& pathname)
{
	std::map<std::string, Datafile*>::iterator i = m_df_cache.find(pathname);
	if (i != m_df_cache.end())
		return i->second;

	// Ensure that the directory for 'pathname' exists
	string pn = str::joinpath(m_path, pathname);
	size_t pos = pn.rfind('/');
	if (pos != string::npos)
		wibble::sys::fs::mkpath(pn.substr(0, pos));

	if (scan::isCompressed(pn))
		throw wibble::exception::Consistency("accessing data file " + pathname,
				"cannot update compressed data files: please manually uncompress it first");

	Datafile* res = new Datafile(pn);
	m_df_cache.insert(make_pair(pathname, res));
	return res;
}

WritableDataset::AcquireResult Writer::acquire(Metadata& md)
{
	// TODO: refuse if md is before "archive age"

	string reldest = (*m_tf)(md) + "." + md.source->format;
	Datafile* df = file(reldest);

	// Try appending
	UItem<types::AssignedDataset> oldads = md.get<types::AssignedDataset>();
	md.set(types::AssignedDataset::create(m_name, ""));

	try {
		df->append(md);
		return ACQ_OK;
	} catch (std::exception& e) {
		// sqlite will take care of transaction consistency
		if (oldads.defined())
			md.set(oldads);
		else
			md.unset(types::TYPE_ASSIGNEDDATASET);
		md.add_note(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
		return ACQ_ERROR;
	}

	// After appending, keep updated info in-memory, and update manifest on
	// flush when the Datafile structures are deallocated
}

bool Writer::replace(Metadata& md)
{
	// Same as acquire
	return acquire(md) == ACQ_OK;
}

void Writer::remove(const std::string& id)
{
	// Nothing to do
}

namespace {
struct Deleter : public maintenance::MaintFileVisitor
{
	std::string name;
	std::ostream& log;
	bool writable;

	Deleter(const std::string& name, std::ostream& log, bool writable)
		: name(name), log(log), writable(writable) {}
	void operator()(const std::string& file, State state)
	{
		if (writable)
		{
			log << name << ": deleting file " << file << endl;
			sys::fs::deleteIfExists(file);
		} else
			log << name << ": would delete file " << file << endl;
	}
};

struct CheckAge : public maintenance::MaintFileVisitor
{
	maintenance::MaintFileVisitor& next;
	const Manifest& idx;
	UItem<types::Time> archive_threshold;
	UItem<types::Time> delete_threshold;

	CheckAge(MaintFileVisitor& next, const Manifest& idx, int archive_age=-1, int delete_age=-1);

	void operator()(const std::string& file, State state);
};

CheckAge::CheckAge(MaintFileVisitor& next, const Manifest& idx, int archive_age, int delete_age)
	: next(next), idx(idx)
{
	time_t now = time(NULL);
	struct tm t;

	// Go to the beginning of the day
	now -= (now % (3600*24));

	if (archive_age != -1)
	{
		time_t arc_thr = now - archive_age * 3600 * 24;
		gmtime_r(&arc_thr, &t);
		archive_threshold = types::Time::create(t);
	}
	if (delete_age != -1)
	{
		time_t del_thr = now - delete_age * 3600 * 24;
		gmtime_r(&del_thr, &t);
		delete_threshold = types::Time::create(t);
	}
}

void CheckAge::operator()(const std::string& file, State state)
{
	if (state != OK or (!archive_threshold.defined() and !delete_threshold.defined()))
		next(file, state);
	else
	{
		UItem<types::Time> start_time;
		UItem<types::Time> end_time;
		idx.fileTimespan(file, start_time, end_time);

		//cerr << "TEST " << maxdate << " WITH " << delete_threshold << " AND " << archive_threshold << endl;
		if (delete_threshold > end_time)
		{
			nag::verbose("CheckAge: %s is old enough to be deleted", file.c_str());
			next(file, TO_DELETE);
		}
		else if (archive_threshold > end_time)
		{
			nag::verbose("CheckAge: %s is old enough to be archived", file.c_str());
			next(file, TO_ARCHIVE);
		}
		else
			next(file, state);
	}
}
}

void Writer::maintenance(maintenance::MaintFileVisitor& v, bool quick)
{
	// TODO Detect if data is not in reftime order

	CheckAge ca(v, *m_mft, m_archive_age, m_delete_age);
	m_mft->check(ca, quick);
	WritableLocal::maintenance(v, quick);
}

void Writer::removeAll(std::ostream& log, bool writable)
{
	Deleter deleter(m_name, log, writable);
	m_mft->check(deleter, true);
	// TODO: empty manifest
	WritableLocal::removeAll(log, writable);
}

void Writer::flush()
{
	for (std::map<std::string, Datafile*>::iterator i = m_df_cache.begin();
			i != m_df_cache.end(); ++i)
	{
		m_mft->acquire(i->first, utils::files::timestamp(i->second->pathname), i->second->sum);
		delete i->second;
	}
	m_df_cache.clear();
	m_mft->flush();
}

void Writer::rescanFile(const std::string& relpath)
{
	// Delete cached info to force a full rescan
	string pathname = str::joinpath(m_path, relpath);
	sys::fs::deleteIfExists(pathname + ".metadata");
	sys::fs::deleteIfExists(pathname + ".summary");

	m_mft->rescanFile(m_path, relpath);
}

namespace {
struct FileCopier : metadata::Consumer
{
	const scan::Validator& m_val;
	std::string dst;
	std::string finalname;
	int fd_dst;
	off_t w_off;

	FileCopier(const scan::Validator& val, const std::string& dst, const std::string& finalname)
		: m_val(val), dst(dst), finalname(finalname), fd_dst(-1), w_off(0)
	{
		fd_dst = open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
		if (fd_dst < 0)
			throw wibble::exception::File(dst, "opening file");
	}
	virtual ~FileCopier()
	{
		flush();
	}

	bool operator()(Metadata& md);
	void flush();
};

bool FileCopier::operator()(Metadata& md)
{
	// Read the data
	wibble::sys::Buffer buf = md.getData();

	// Check it for corruption
	m_val.validate(buf.data(), buf.size());

	// Write it out
	ssize_t res = write(fd_dst, buf.data(), buf.size());
	if (res < 0 || (unsigned)res != buf.size())
		throw wibble::exception::File(dst, "writing " + str::fmt(buf.size()) + " bytes");

	// Update the Blob source using the new position
	md.source = types::source::Blob::create(md.source->format, finalname, w_off, buf.size());

	w_off += buf.size();

	return true;
}

void FileCopier::flush()
{
	if (fd_dst != -1)
	{
		if (fdatasync(fd_dst) != 0)
			throw wibble::exception::File(dst, "flushing data to file");
		if (close(fd_dst) != 0)
			throw wibble::exception::File(dst, "closing file");
		fd_dst = -1;
	}
}
}

size_t Writer::repackFile(const std::string& relpath)
{
	string pathname = str::joinpath(m_path, relpath);

	// Read the metadata
	metadata::Collection mdc;
	scan::scan(pathname, mdc);

	// Sort by reference time
	mdc.sort();

	// Write out the data with the new order
	string pntmp = pathname + ".repack";
	FileCopier copier(scan::Validator::by_filename(pathname), pntmp, str::basename(relpath));
	mdc.sendTo(copier);
	copier.flush();

	// Prevent reading the still open old file using the new offsets
	Metadata::flushDataReaders();

	// Remove existing cached metadata, since we scramble their order
	sys::fs::deleteIfExists(pathname + ".metadata");
	sys::fs::deleteIfExists(pathname + ".summary");

	size_t size_pre = files::size(pathname);
	size_t size_post = files::size(pntmp);
	
	// Rename the data file with to final name
	if (rename(pntmp.c_str(), pathname.c_str()) < 0)
		throw wibble::exception::System("renaming " + pntmp + " to " + pathname);

	// Write out the new metadata
	mdc.writeAtomically(pathname + ".metadata");

	// Regenerate the summary. It is unchanged, really, but its timestamp
	// has become obsolete by now
	Summary sum;
	metadata::Summarise mds(sum);
	mdc.sendTo(mds);
	sum.writeAtomically(pathname + ".summary");

	// Reindex with the new file information
	time_t mtime = files::timestamp(pathname);
	m_mft->acquire(relpath, mtime, sum);

	return size_pre - size_post;
}

size_t Writer::removeFile(const std::string& relpath, bool withData)
{
	m_mft->remove(relpath);
	if (withData)
	{
		string pathname = str::joinpath(m_path, relpath);
		size_t size = files::size(pathname);
		if (unlink(pathname.c_str()) < 0)
			throw wibble::exception::System("removing " + pathname);
		return size;
	} else
		return 0;
}

void Writer::archiveFile(const std::string& relpath)
{
	// Remove from index
	m_mft->remove(relpath);

	// Delegate the rest to WritableLocal
	WritableLocal::archiveFile(relpath);
}

size_t Writer::vacuum()
{
	// Nothing to do here really
	return 0;
}

WritableDataset::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	// TODO
#if 0
	wibble::sys::fs::Lockfile lockfile(wibble::str::joinpath(cfg.value("path"), "lock"));

	string name = cfg.value("name");
	try {
		if (ConfigFile::boolValue(cfg.value("replace")))
		{
			if (cfg.value("index") != string())
				ondisk::writer::IndexedRootDirectory::testReplace(cfg, md, out);
			else
				ondisk::writer::RootDirectory::testReplace(cfg, md, out);
			return ACQ_OK;
		} else {
			try {
				if (cfg.value("index") != string())
					ondisk::writer::IndexedRootDirectory::testAcquire(cfg, md, out);
				else
					ondisk::writer::RootDirectory::testAcquire(cfg, md, out);
				return ACQ_OK;
			} catch (Index::DuplicateInsert& di) {
				out << "Source information restored to original value" << endl;
				out << "Failed to store in dataset '"+name+"' because the dataset already has the data: " + di.what() << endl;
				return ACQ_ERROR_DUPLICATE;
			}
		}
	} catch (std::exception& e) {
		out << "Source information restored to original value" << endl;
		out << "Failed to store in dataset '"+name+"': " + e.what() << endl;
		return ACQ_ERROR;
	}
#endif
}


}
}
}

// vim:set ts=4 sw=4:
