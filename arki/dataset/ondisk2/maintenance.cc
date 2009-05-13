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

#include <arki/dataset/ondisk2/maintenance.h>
//#include <arki/dataset/ondisk2/maint/datafile.h>
//#include <arki/dataset/ondisk2/maint/directory.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/index.h>
#include <arki/utils/files.h>

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
		case TO_PACK: cerr << file << " HOLES" << endl;
		case TO_INDEX: cerr << file << " OUT_OF_INDEX" << endl;
		case TO_RESCAN: cerr << file << " CORRUPTED" << endl;
	}
}

// Repacker

Repacker::Repacker(std::ostream& log, Writer& w)
	: m_log(log), w(w)
{
}

std::ostream& Repacker::log()
{
	return m_log << w.m_name << ": ";
}

// FailsafeRepacker

FailsafeRepacker::FailsafeRepacker(std::ostream& log, Writer& w)
	: Repacker(log, w), m_count_deleted(0)
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

// RealRepacker

RealRepacker::RealRepacker(std::ostream& log, Writer& w)
	: Repacker(log, w), m_count_packed(0), m_count_deleted(0)
{
}

void RealRepacker::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK: {
			// Lock away writes and reads reads
			Pending p = w.m_idx.beginExclusiveTransaction();

			// Make a copy of the file with the right data in it, sorted by
			// reftime, and update the offsets in the index
			string pathname = str::joinpath(w.m_path, file);
			string pntmp = pathname + ".repack";
			FileCopier copier(w.m_idx, pathname, pntmp);
			w.m_idx.scan_file(file, copier, "reftime");
			copier.flush();

			size_t size_pre = utils::files::size(pathname);
			size_t size_post = utils::files::size(pntmp);

			// Rename the file with to final name
			if (rename(pntmp.c_str(), pathname.c_str()) < 0)
				throw wibble::exception::System("renaming " + pntmp + " to " + pathname);

			// Commit the changes on the database
			p.commit();

			size_t saved = size_pre - size_post;
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

	if (m_count_packed > 0 || m_count_deleted > 0 || saved > 0)
		m_log << w.m_name << ": ";
	if (m_count_packed)
		m_log << m_count_packed << "files packed, ";
	if (m_count_deleted)
		m_log << m_count_deleted << "files deleted, ";
	if (saved > 0)
		m_log << saved << "bytes reclaimed on the index, ";
	if (m_count_freed > 0)
		m_log << m_count_freed << " total bytes freed." << endl;
}

MockRepacker::MockRepacker(std::ostream& log, Writer& w)
	: Repacker(log, w), m_count_packed(0), m_count_deleted(0)
{
}

void MockRepacker::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case TO_PACK: {
			log() << file << " needs packing" << endl;
			++m_count_packed;
			break;
		}
		case TO_INDEX: {
			log() << file << " should be deleted" << endl;
			++m_count_deleted;
			break;
		}
		default:
			break;
	}
}

void MockRepacker::end()
{
	if (m_count_packed)
		if (m_count_deleted)
			log()
			    << m_count_packed << " files should be packed, "
			    << m_count_deleted << " files should be deleted."
			    << endl;
		else
			log()
			    << m_count_packed << " files should be packed. "
			    << endl;
	else if (m_count_deleted)
		log()
		    << m_count_deleted << " files should be deleted."
		    << endl;
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
