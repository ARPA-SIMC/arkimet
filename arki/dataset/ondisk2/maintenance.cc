/*
 * dataset/ondisk2/maintenance - Ondisk2 dataset maintenance
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/ondisk2/maintenance.h>
//#include <arki/dataset/ondisk2/maint/datafile.h>
//#include <arki/dataset/ondisk2/maint/directory.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/index.h>
#include <arki/utils.h>
#include <arki/utils/files.h>

#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif
#ifdef HAVE_DBALLE
#include <arki/scan/bufr.h>
#endif

#include <wibble/sys/fs.h>

#include <ostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


using namespace std;
using namespace wibble;
using namespace arki::dataset::ondisk2::maint;

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {

void HoleFinder::finaliseFile()
{
	if (!last_file.empty())
	{
		// Check if last_file_size matches the file size
		if (!has_hole)
		{
			off_t size = utils::files::size(str::joinpath(m_root, last_file));
			if (size > last_file_size)
				has_hole = true;
			else if (size < last_file_size)
			{
				// throw wibble::exception::Consistency("checking size of "+last_file, "file is shorter than what the index believes: please run a dataset check");
				next(last_file, writer::MaintFileVisitor::TO_RESCAN);
				return;
			}
		}

		// Take note of files with holes
		if (has_hole)
		{
			next(last_file, writer::MaintFileVisitor::TO_PACK);
		} else {
			next(last_file, writer::MaintFileVisitor::OK);
		}
	}
}


FileCopier::FileCopier(WIndex& idx, const std::string& src, const std::string& dst)
	: m_idx(idx), src(src), dst(dst), fd_src(-1), fd_dst(-1), w_off(0)
{
	fd_src = open(src.c_str(), O_RDONLY | O_NOATIME);
	if (fd_src < 0)
		throw wibble::exception::File(src, "opening file");

	fd_dst = open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd_dst < 0)
		throw wibble::exception::File(dst, "opening file");
}

FileCopier::~FileCopier()
{
	flush();
}

void FileCopier::operator()(const std::string& file, off_t offset, size_t size)
{
	if (buf.size() < size)
		buf.resize(size);
	ssize_t res = pread(fd_src, buf.data(), size, offset);
	if (res < 0 || (unsigned)res != size)
		throw wibble::exception::File(src, "reading " + str::fmt(size) + " bytes");
	res = write(fd_dst, buf.data(), size);
	if (res < 0 || (unsigned)res != size)
		throw wibble::exception::File(dst, "writing " + str::fmt(size) + " bytes");

	// Reindex file from offset to w_off
	m_idx.relocate_data(file, offset, w_off);

	w_off += size;
}

void FileCopier::flush()
{
	if (fd_src != -1 and close(fd_src) != 0)
		throw wibble::exception::File(src, "closing file");
	fd_src = -1;
	if (fd_dst != -1 and close(fd_dst) != 0)
		throw wibble::exception::File(dst, "closing file");
	fd_dst = -1;
}

void MaintPrinter::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case OK: cerr << file << " OK" << endl;
		case TO_PACK: cerr << file << " TO PACK" << endl;
		case TO_INDEX: cerr << file << " TO INDEX" << endl;
		case TO_RESCAN: cerr << file << " TO RESCAN" << endl;
		case DELETED: cerr << file << " DELETED" << endl;
	}
}

// Agent

Agent::Agent(std::ostream& log, Writer& w)
	: m_log(log), w(w), lineStart(true)
{
}

std::ostream& Agent::log()
{
	return m_log << w.m_name << ": ";
}

void Agent::logStart()
{
	lineStart = true;
}

std::ostream& Agent::logAdd()
{
	if (lineStart)
	{
		lineStart = false;
		return m_log << w.m_name << ": ";
	}
	else
		return m_log << ", ";
}

void Agent::logEnd()
{
	if (! lineStart)
		m_log << "." << endl;
}

// FailsafeRepacker

FailsafeRepacker::FailsafeRepacker(std::ostream& log, Writer& w)
	: Agent(log, w), m_count_deleted(0)
{
}

void FailsafeRepacker::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_INDEX: ++m_count_deleted; break;
		default: break;
	}
}

void FailsafeRepacker::end()
{
	if (m_count_deleted == 0) return;
	log() << "index is empty, skipping deletion of "
	      << m_count_deleted << " files."
	      << endl;
}

// Common maintenance functions

static size_t repack(const std::string& root, const std::string& file, WIndex& idx)
{
	// Lock away writes and reads
	Pending p = idx.beginExclusiveTransaction();

	// Make a copy of the file with the right data in it, sorted by
	// reftime, and update the offsets in the index
	string pathname = str::joinpath(root, file);
	string pntmp = pathname + ".repack";
	FileCopier copier(idx, pathname, pntmp);
	idx.scan_file(file, copier, "reftime");
	copier.flush();

	size_t size_pre = utils::files::size(pathname);
	size_t size_post = utils::files::size(pntmp);

	// Rename the file with to final name
	if (rename(pntmp.c_str(), pathname.c_str()) < 0)
		throw wibble::exception::System("renaming " + pntmp + " to " + pathname);

	// Commit the changes on the database
	p.commit();

	return size_pre - size_post;
}

static void scan_metadata(const std::string& file, MetadataConsumer& c)
{
	cerr << "Reading cached metadata from " << file << endl;
	Metadata::readFile(file, c);
}

static void scan_file(const std::string& file, MetadataConsumer& c)
{
	// Get the file extension
	size_t pos = file.rfind('.');
	if (pos == string::npos)
		throw wibble::exception::Consistency("getting extension of " + file, "file name has no extension");
	string ext = file.substr(pos+1);

	// Rescan the file and regenerate the metadata file
	bool processed = false;
#ifdef HAVE_GRIBAPI
	if (ext == "grib1" || ext == "grib2")
	{
		scan::Grib scanner;
		scanner.open(file);
		Metadata md;
		while (scanner.next(md))
			c(md);
		processed = true;
	}
#endif
#ifdef HAVE_DBALLE
	if (ext == "bufr") {
		scan::Bufr scanner;
		scanner.open(file);
		Metadata md;
		while (scanner.next(md))
			c(md);
		processed = true;
	}
#endif
	if (!processed)
		throw wibble::exception::Consistency("rebuilding " + file, "rescanning \"" + ext + "\" is not yet implemented");
}

struct Reindexer : public MetadataConsumer
{
	WIndex& idx;
	std::string dsname;
	std::string relfile;
	std::string basename;
	MetadataConsumer& salvage;
	size_t count_salvaged;

	Reindexer(WIndex& idx,
		  const std::string& dsname,
		  const std::string& relfile,
		  MetadataConsumer& salvage)
	       	: idx(idx), dsname(dsname), relfile(relfile), basename(str::basename(relfile)),
	          salvage(salvage), count_salvaged(0) {}

	virtual bool operator()(Metadata& md)
	{
		Item<types::source::Blob> blob = md.source.upcast<types::source::Blob>();
		try {
			int id;
			if (str::basename(blob->filename) != basename)
				throw wibble::exception::Consistency(
					"rescanning " + relfile,
				       	"metadata points to the wrong file: " + blob->filename);
			idx.index(md, relfile, blob->offset, &id);
		} catch (index::DuplicateInsert& di) {
			md.notes.push_back(types::Note::create("Failed to reindex in dataset '"+dsname+"' because the dataset already has the data: " + di.what()));
			++count_salvaged;
			salvage(md);
		} catch (std::exception& e) {
			// sqlite will take care of transaction consistency
			md.notes.push_back(types::Note::create("Failed to reindex in dataset '"+dsname+"': " + e.what()));
			++count_salvaged;
			salvage(md);
		}
	}
};

static size_t rescan(const std::string& dsname, const std::string& root, const std::string& file, WIndex& idx, MetadataConsumer& salvage)
{
	// Lock away writes and reads
	Pending p = idx.beginExclusiveTransaction();

	// Remove from the index all data about the file
	idx.reset(file);

	string pathname = str::joinpath(root, file);

	Reindexer fixer(idx, dsname, file, salvage);
	if (utils::hasFlagfile(pathname + ".metadata"))
	{
		// If there is a metadata file, use it to save time
		// (this is very useful when converting ondisk datasets to
		// ondisk2)
		scan_metadata(pathname + ".metadata", fixer);
	} else {
		// TODO: check from a list of extensions what is that we can scan
		// Rescan the file
		scan_file(pathname, fixer);
	}

	// TODO: if scan fails, remove all info from the index and rename the
	// file to something like .broken

	// Commit the changes on the database
	p.commit();

	return fixer.count_salvaged;
}

// RealRepacker

RealRepacker::RealRepacker(std::ostream& log, Writer& w)
	: Agent(log, w), m_count_packed(0), m_count_deleted(0), m_count_deindexed(0), m_count_freed(0)
{
}

void RealRepacker::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK: {
		        // Repack the file
			size_t saved = repack(w.m_path, file, w.m_idx);
			log() << "packed " << file << " (" << saved << " saved)" << endl;
			++m_count_packed;
			m_count_freed += saved;
			break;
		}
		case TO_INDEX: {
			// Delete all files not indexed
			string pathname = str::joinpath(w.m_path, file);
			size_t size = utils::files::size(pathname);
			if (unlink(pathname.c_str()) < 0)
				throw wibble::exception::System("removing " + pathname);

			log() << "deleted " << file << " (" << size << " freed)" << endl;
			++m_count_deleted;
			m_count_freed += size;
			break;
		}
		case DELETED: {
			// Remove from index those files that have been deleted
			w.m_idx.reset(file);
			++m_count_deindexed;
			break;
	        }
		default:
			break;
	}
}

void RealRepacker::end()
{
	// Finally, tidy up the database
	size_t size_pre = utils::files::size(w.m_idx.pathname());
	w.m_idx.vacuum();
	size_t size_post = utils::files::size(w.m_idx.pathname());
	size_t saved = size_pre - size_post;
	m_count_freed += saved;

	logStart();
	if (m_count_packed)
		logAdd() << m_count_packed << " files packed";
	if (m_count_deleted)
		logAdd() << m_count_deleted << " files deleted";
	if (m_count_deindexed)
		logAdd() << m_count_deindexed << " files removed from index";
	if (saved > 0)
		logAdd() << saved << " bytes reclaimed on the index";
	if (m_count_freed > 0)
		logAdd() << m_count_freed << " total bytes freed";
	logEnd();
}

// MockRepacker

MockRepacker::MockRepacker(std::ostream& log, Writer& w)
	: Agent(log, w), m_count_packed(0), m_count_deleted(0), m_count_deindexed(0)
{
}

void MockRepacker::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK:
			log() << file << " should be packed" << endl;
			++m_count_packed;
			break;
		case TO_INDEX:
			log() << file << " should be deleted" << endl;
			++m_count_deleted;
			break;
		case DELETED:
			log() << file << " should be removed from the index" << endl;
			++m_count_deindexed;
			break;
		default:
			break;
	}
}

void MockRepacker::end()
{
	logStart();
	if (m_count_packed)
		logAdd() << m_count_packed << " files should be packed";
	if (m_count_deleted)
		logAdd() << m_count_deleted << " files should be deleted";
	if (m_count_deindexed)
		logAdd() << m_count_deleted << " files should be removed from the index";
	logEnd();
}

// RealFixer

RealFixer::RealFixer(std::ostream& log, Writer& w, MetadataConsumer& salvage)
	: Agent(log, w), salvage(salvage),
          m_count_packed(0), m_count_rescanned(0), m_count_deindexed(0), m_count_salvaged(0)
{
}

void RealFixer::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK: {
		        // Repack the file
			size_t saved = repack(w.m_path, file, w.m_idx);
			log() << "packed " << file << " (" << saved << " saved)" << endl;
			++m_count_packed;
			break;
		}
		case TO_INDEX:
		case TO_RESCAN: {
			// Skip .metadata files
			if (str::endsWith(file, ".metadata")) break;
			m_count_salvaged += rescan(w.m_name, w.m_path, file, w.m_idx, salvage);
			++m_count_rescanned;
			break;
		}
		case DELETED: {
			// Remove from index those files that have been deleted
			w.m_idx.reset(file);
			++m_count_deindexed;
			break;
	        }
		default:
			break;
	}
}

void RealFixer::end()
{
	// Finally, tidy up the database
	size_t size_pre = utils::files::size(w.m_idx.pathname());
	w.m_idx.vacuum();
	size_t size_post = utils::files::size(w.m_idx.pathname());
	size_t saved = size_pre - size_post;

	logStart();
	if (m_count_packed)
		logAdd() << m_count_packed << " files packed";
	if (m_count_rescanned)
		logAdd() << m_count_rescanned << " files rescanned";
	if (m_count_deindexed)
		logAdd() << m_count_deindexed << " files removed from index";
	if (saved > 0)
		logAdd() << saved << " bytes reclaimed cleaning the index";
	if (m_count_salvaged)
		logAdd() << m_count_salvaged << " data items could not be reindexed";
	logEnd();
}

// MockFixer

MockFixer::MockFixer(std::ostream& log, Writer& w)
	: Agent(log, w), m_count_packed(0), m_count_rescanned(0), m_count_deindexed(0)
{
}

void MockFixer::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK:
			log() << file << " should be packed" << endl;
			++m_count_packed;
			break;
		case TO_INDEX:
		case TO_RESCAN:
			// Skip .metadata files
			if (str::endsWith(file, ".metadata")) break;
			log() << file << " should be rescanned" << endl;
			++m_count_rescanned;
			break;
		case DELETED:
			log() << file << " should be removed from the index" << endl;
			++m_count_deindexed;
			break;
		default:
			break;
	}
}

void MockFixer::end()
{
	logStart();
	if (m_count_packed)
		logAdd() << m_count_packed << " files should be packed";
	if (m_count_rescanned)
		logAdd() << m_count_rescanned << " files should be rescanned";
	if (m_count_deindexed)
		logAdd() << m_count_deindexed << " files should be removed from the index";
	logEnd();
}

}

#if 0
FullMaintenance::FullMaintenance(std::ostream& log, MetadataConsumer& salvage)
	: log(log), salvage(salvage), reindexAll(false), writer(0)
{
}

FullMaintenance::~FullMaintenance()
{
}

void FullMaintenance::start(Writer& w)
{
	writer = &w;
	reindexAll = true;
}
void FullMaintenance::end()
{
	if (writer)
	{
		writer->flush();
		writer = 0;
	}
}

void FullMaintenance::needsDatafileRebuild(Datafile& df)
{
	// Run the rebuild
	log << df.pathname << ": rebuilding metadata." << endl;
	df.rebuild(salvage, reindexAll);
}

void FullMaintenance::needsSummaryRebuild(Datafile& df)
{
	log << df.pathname << ": rebuilding summary." << endl;
	df.rebuildSummary(reindexAll);
}

void FullMaintenance::needsReindex(maint::Datafile& df)
{
	log << df.pathname << ": reindexing data." << endl;
	df.reindex(salvage);
}

void FullMaintenance::needsSummaryRebuild(Directory& df)
{
	log << df.fullpath() << ": rebuilding summary." << endl;
	df.rebuildSummary();
}

void FullMaintenance::needsFullIndexRebuild(RootDirectory& df)
{
	if (writer)
	{
		log << writer->path() << ": fully rebuilding index." << endl;
		// Delete the index and recreate it new
		df.recreateIndex();
		// Delete all summary files
		df.deleteSummaries();
		// Set a flag saying that we need to reindex every file
		reindexAll = true;
	}
}


FullRepack::FullRepack(MetadataConsumer& mdc, std::ostream& log)
	: mdc(mdc), log(log), writer(0)
{
}

FullRepack::~FullRepack()
{
}

void FullRepack::start(Writer& w)
{
	writer = &w;
}
void FullRepack::end()
{
	writer = 0;
}

void FullRepack::needsRepack(Datafile& df)
{
	// Run the rebuild
	log << df.pathname << ": repacking." << endl;
	df.repack(mdc);
	// TODO: Reindex
}
#endif

}
}
}
// vim:set ts=4 sw=4:
