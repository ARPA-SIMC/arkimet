/*
 * dataset/maintenance - Dataset maintenance utilities
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/maintenance.h>
#include <arki/dataset/local.h>
#include <arki/dataset/archive.h>
#include <arki/metadata/collection.h>
#include <arki/dataset/data.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/compress.h>
#include <arki/scan/any.h>
#include <arki/sort.h>
#include <arki/nag.h>

#include <wibble/sys/fs.h>

#include <algorithm>
#include <ostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctime>
#include <cstdio>

// Be compatible with systems without O_NOATIME
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

template<typename T>
static std::string nfiles(const T& val)
{
	if (val == 1)
		return "1 file";
	else
		return str::fmt(val) + " files";
}

namespace arki {
namespace dataset {
namespace maintenance {

namespace {
struct FileInfo
{
    std::string name;
    off_t checked;
    const data::Info* format_info;
    const scan::Validator* validator;
    int validator_fd;
    bool has_hole;
    bool corrupted;

    FileInfo(const std::string& name, bool quick=true)
        : name(name), checked(0), format_info(0), validator(0), validator_fd(-1), has_hole(false), corrupted(false)
    {
        // Get the file extension
        std::string fmt;
        size_t pos;
        if ((pos = name.rfind('.')) != std::string::npos)
            fmt = name.substr(pos + 1);
        format_info = data::Info::get(fmt);

        if (!quick)
        {
            // If the data file is compressed, create a temporary uncompressed copy
            auto_ptr<utils::compress::TempUnzip> tu;
            if (scan::isCompressed(name))
                tu.reset(new utils::compress::TempUnzip(name));

            validator = &scan::Validator::by_filename(name.c_str());
            validator_fd = open(name.c_str(), O_RDONLY);
            if (validator_fd < 0)
            {
                perror(name.c_str());
                corrupted = true;
            } else
                (void)posix_fadvise(validator_fd, 0, 0, POSIX_FADV_DONTNEED);
        }
    }

    void check_data(off_t offset, size_t size);
    unsigned finalise();
};

void FileInfo::check_data(off_t offset, size_t size)
{
    // If we've already found that the file is corrupted, there is nothing
    // else to do
    if (corrupted) return;

    if (validator)
        try {
            validator->validate(validator_fd, offset, size, name);
        } catch (wibble::exception::Generic& e) {
            nag::warning("corruption detected at %s:%ld-%zd: %s", name.c_str(), offset, size, e.desc().c_str());
            corrupted = true;
        }

    format_info->raw_to_wrapped(offset, size);
    if (offset > checked)
    {
        has_hole = true;
    }
    checked = offset + size;
}

unsigned FileInfo::finalise()
{
    off_t fsize;

    if (validator_fd >= 0)
    {
        close(validator_fd);
        validator_fd = -1;
    }

    if (corrupted)
    {
        nag::verbose("HoleFinder: %s found corrupted", name.c_str());
        return MaintFileVisitor::TO_RESCAN;
    }

    fsize = compress::filesize(name);
    off_t d_offset = 0;
    size_t d_size = fsize;
    if (d_size < checked)
    {
        nag::verbose("HoleFinder: %s found truncated (%zd < %zd bytes)", name.c_str(), fsize, checked);
        // throw wibble::exception::Consistency("checking size of "+last_file, "file is shorter than what the index believes: please run a dataset check");
        return MaintFileVisitor::TO_RESCAN;
    }

    // Check if last_file_size matches the file size
    if (d_size > checked)
    {
        has_hole = true;
    }

    // Take note of files with holes
    if (has_hole)
    {
        nag::verbose("HoleFinder: %s contains deleted data", name.c_str());
        return MaintFileVisitor::TO_PACK;
    } else {
        return MaintFileVisitor::OK;
    }
}

}

HoleFinder::HoleFinder(MaintFileVisitor& next, const std::string& root, bool quick)
    : next(next), m_root(root), quick(quick)
{
}

void HoleFinder::operator()(const std::string& file, const metadata::Collection& mdc)
{
    string fname = str::joinpath(m_root, file);
    FileInfo info(fname, quick);

    for (metadata::Collection::const_iterator i = mdc.begin(); i != mdc.end(); ++i)
    {
        Item<types::source::Blob> source = i->source.upcast<types::source::Blob>();
        info.check_data(source->offset, source->size);
    }

    unsigned response = info.finalise();
    next(file, response);
}

void HoleFinder::scan(const std::string& file)
{
	struct HFSorter : public sort::Compare
	{
		virtual int compare(const Metadata& a, const Metadata& b) const {
			int res = a.get(types::TYPE_REFTIME).compare(b.get(types::TYPE_REFTIME));
			if (res == 0)
				return a.source.compare(b.source);
			return res;
		}
		virtual std::string toString() const {
			return "HFSorter";
		}
	} cmp;

	string fname = str::joinpath(m_root, file);

	// If the data file is compressed, create a temporary uncompressed copy
	auto_ptr<utils::compress::TempUnzip> tu;
	if (!quick && scan::isCompressed(fname))
		tu.reset(new utils::compress::TempUnzip(fname));

	metadata::Collection mdc;
	scan::scan(fname, mdc);
	//mdc.sort(""); // Sort by reftime, to find items out of order
	mdc.sort(cmp); // Sort by reftime and by offset
    (*this)(file, mdc);
}

static bool sorter(const std::string& a, const std::string& b)
{
	return b < a;
}

FindMissing::FindMissing(MaintFileVisitor& next, const std::vector<std::string>& files)
	: next(next), disk(files)
{
	// Sort backwards because we read from the end
	std::sort(disk.begin(), disk.end(), sorter);
}

void FindMissing::operator()(const std::string& file, unsigned state)
{
    while (not disk.empty() and disk.back() < file)
    {
        nag::verbose("FindMissing: %s is not in index", disk.back().c_str());
        next(disk.back(), state | TO_INDEX);
        disk.pop_back();
    }
    if (!disk.empty() && disk.back() == file)
    {
        // TODO: if requested, check for internal consistency
        // TODO: it requires to have an infrastructure for quick
        // TODO:   consistency checkers (like, "GRIB starts with GRIB
        // TODO:   and ends with 7777")

        disk.pop_back();
        next(file, state);
    }
    else // if (disk.empty() || disk.back() > file)
    {
        nag::verbose("FindMissing: %s has been deleted", file.c_str());
        next(file, (state & ~TO_RESCAN) | TO_DEINDEX);
    }
}

void FindMissing::end()
{
    while (not disk.empty())
    {
        nag::verbose("FindMissing: %s is not in index", disk.back().c_str());
        next(disk.back(), TO_INDEX);
        disk.pop_back();
    }
}

void Dumper::operator()(const std::string& file, unsigned state)
{
    string res = file;
    if (state == OK)        res += " OK";
    if (state & TO_ARCHIVE) res += " TO_ARCHIVE";
    if (state & TO_DELETE)  res += " TO_DELETE";
    if (state & TO_PACK)    res += " TO_PACK";
    if (state & TO_INDEX)   res += " TO_INDEX";
    if (state & TO_RESCAN)  res += " TO_RESCAN";
    if (state & TO_DEINDEX) res += " TO_DEINDEX";
    if (state & ARCHIVED)   res += " ARCHIVED";
    cerr << res << endl;
}

Tee::Tee(MaintFileVisitor& one, MaintFileVisitor& two) : one(one), two(two) {}
Tee::~Tee() {}
void Tee::operator()(const std::string& file, unsigned state)
{
	one(file, state);
	two(file, state);
}

// Agent

Agent::Agent(std::ostream& log, WritableLocal& w)
	: m_log(log), w(w), lineStart(true)
{
}

std::ostream& Agent::log()
{
	return m_log << w.name() << ": ";
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
		return m_log << w.name() << ": ";
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

FailsafeRepacker::FailsafeRepacker(std::ostream& log, WritableLocal& w)
	: Agent(log, w), m_count_deleted(0)
{
}

void FailsafeRepacker::operator()(const std::string& file, unsigned state)
{
    if (state & TO_INDEX) ++m_count_deleted;
}

void FailsafeRepacker::end()
{
	if (m_count_deleted == 0) return;
	log() << "index is empty, skipping deletion of "
	      << m_count_deleted << " files."
	      << endl;
}

// MockRepacker

MockRepacker::MockRepacker(std::ostream& log, WritableLocal& w)
	: Agent(log, w), m_count_packed(0), m_count_archived(0),
	  m_count_deleted(0), m_count_deindexed(0), m_count_rescanned(0)
{
}

void MockRepacker::operator()(const std::string& file, unsigned state)
{
    if (state & TO_PACK && !(state & TO_DELETE))
    {
        log() << file << " should be packed" << endl;
        ++m_count_packed;
    }
    if (state & TO_ARCHIVE)
    {
        log() << file << " should be archived" << endl;
        ++m_count_archived;
    }
    if (state & TO_DELETE)
    {
        log() << file << " should be deleted and removed from the index" << endl;
        ++m_count_deleted;
        ++m_count_deindexed;
    }
    if (state & TO_INDEX)
    {
        ostream& l = log() << file << " should be deleted";
        if (state & ARCHIVED) l << " from the archive" << endl;
        l << endl;
        ++m_count_deleted;
    }
    if (state & TO_DEINDEX)
    {
        ostream& l = log() << file << " should be removed from the";
        if (state & ARCHIVED) l << " archive";
        l << " index" << endl;
        ++m_count_deindexed;
    }
    if (state & TO_RESCAN || state & ARCHIVED)
    {
        log() << file << " should be rescanned by the archive" << endl;
        ++m_count_rescanned;
    }
}

void MockRepacker::end()
{
	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " should be packed";
	if (m_count_archived)
		logAdd() << nfiles(m_count_archived) << " should be archived";
	if (m_count_deleted)
		logAdd() << nfiles(m_count_deleted) << " should be deleted";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " should be removed from the index";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " should be rescanned";
	logEnd();
}

// MockFixer

MockFixer::MockFixer(std::ostream& log, WritableLocal& w)
	: Agent(log, w), m_count_packed(0), m_count_rescanned(0), m_count_deindexed(0)
{
}

void MockFixer::operator()(const std::string& file, unsigned state)
{
    if (state & TO_PACK)
    {
        log() << file << " should be packed" << endl;
        ++m_count_packed;
    }
    if (state & (TO_INDEX | TO_RESCAN))
    {
        ostream& l = log() << file << " should be rescanned";
        if (state & ARCHIVED)
            l << " by the archive";
        l << endl;
        ++m_count_rescanned;
    }
    if (state & TO_DEINDEX)
    {
        ostream& l = log() << file << " should be removed";
        if (state & ARCHIVED)
            l << " from the archive";
        l << endl;
        ++m_count_deindexed;
    }
}

void MockFixer::end()
{
	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " should be packed";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " should be rescanned";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " should be removed from the index";
	logEnd();
}

// RealRepacker

RealRepacker::RealRepacker(std::ostream& log, WritableLocal& w)
	: Agent(log, w), m_count_packed(0), m_count_archived(0),
	  m_count_deleted(0), m_count_deindexed(0), m_count_rescanned(0),
	  m_count_freed(0), m_touched_archive(false), m_redo_summary(false)
{
}

void RealRepacker::operator()(const std::string& file, unsigned state)
{
    if (state & TO_PACK && !(state & TO_DELETE))
    {
        // Repack the file
        size_t saved = w.repackFile(file);
        log() << "packed " << file << " (" << saved << " saved)" << endl;
        ++m_count_packed;
        m_count_freed += saved;
    }
    if (state & TO_ARCHIVE)
    {
        // Create the target directory in the archive
        w.archiveFile(file);
        log() << "archived " << file << endl;
        ++m_count_archived;
        m_touched_archive = true;
        m_redo_summary = true;
    }
    if (state & TO_DELETE)
    {
        // Delete obsolete files
        size_t size = w.removeFile(file, true);
        log() << "deleted " << file << " (" << size << " freed)" << endl;
        ++m_count_deleted;
        ++m_count_deindexed;
        m_count_freed += size;
        m_redo_summary = true;
    }
    if (state & ARCHIVED)
    {
        if (state & TO_INDEX || state & TO_RESCAN)
        {
            /// File is not present in the archive index
            /// File contents need reindexing in the archive
            w.archive().rescan(file);
            log() << "rescanned in archive " << file << endl;
            ++m_count_rescanned;
            m_touched_archive = true;
        }
        if (state & TO_DEINDEX)
        {
            w.archive().remove(file);
            log() << "deleted from archive index " << file << endl;
            ++m_count_deindexed;
            m_touched_archive = true;
        }
    } else {
        if (state & TO_INDEX)
        {
            // Delete all files not indexed
            size_t size = w.removeFile(file, true);
            log() << "deleted " << file << " (" << size << " freed)" << endl;
            ++m_count_deleted;
            m_count_freed += size;
        }
        if (state & TO_DEINDEX)
        {
            // Remove from index those files that have been deleted
            w.removeFile(file, false);
            log() << "deleted from index " << file << endl;
            ++m_count_deindexed;
            m_redo_summary = true;
        }
    }
}

void RealRepacker::end()
{
	if (m_touched_archive)
	{
		w.archive().vacuum();
		log() << "archive cleaned up" << endl;
	}

	// Finally, tidy up the database
	size_t size_vacuum = w.vacuum();

	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " packed";
	if (m_count_archived)
		logAdd() << nfiles(m_count_archived) << " archived";
	if (m_count_deleted)
		logAdd() << nfiles(m_count_deleted) << " deleted";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " removed from index";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " rescanned";
	if (size_vacuum)
	{
		//logAdd() << size_vacuum << " bytes reclaimed on the index";
		m_count_freed += size_vacuum;
	}
	if (m_count_freed > 0)
		logAdd() << m_count_freed << " total bytes freed";
	logEnd();
}

// RealFixer

RealFixer::RealFixer(std::ostream& log, WritableLocal& w)
	: Agent(log, w), 
          m_count_packed(0), m_count_rescanned(0), m_count_deindexed(0),
	  m_touched_archive(false), m_redo_summary(false)
{
}

void RealFixer::operator()(const std::string& file, unsigned state)
{
    /* Packing is left to the repacker, during check we do not
     * mangle the data files
    case TO_PACK: {
            // Repack the file
            size_t saved = repack(w.path(), file, w.m_idx);
            log() << "packed " << file << " (" << saved << " saved)" << endl;
            ++m_count_packed;
            break;
    }
    */
    if (state & ARCHIVED)
    {
        if (state & TO_INDEX || state & TO_RESCAN)
        {
            /// File is not present in the archive index
            /// File contents need reindexing in the archive
            w.archive().rescan(file);
            log() << "rescanned in archive " << file << endl;
            ++m_count_rescanned;
            m_touched_archive = true;
        }
        if (state & TO_DEINDEX)
        {
            /// File does not exist, but has entries in the archive index
            w.archive().remove(file);
            log() << "deindexed in archive " << file << endl;
            ++m_count_deindexed;
            m_touched_archive = true;
        }
    } else {
        if (state & TO_INDEX || state & TO_RESCAN)
        {
            w.rescanFile(file);
            log() << "rescanned " << file << endl;
            ++m_count_rescanned;
            m_redo_summary = true;
        }
        if (state & TO_DEINDEX)
        {
            // Remove from index those files that have been deleted
            w.removeFile(file, false);
            log() << "deindexed " << file << endl;
            ++m_count_deindexed;
            m_redo_summary = true;
        }
    }
}

void RealFixer::end()
{
	if (m_touched_archive)
	{
		w.archive().vacuum();
		log() << "archive cleaned up" << endl;
	}

	// Finally, tidy up the database
	size_t size_vacuum = w.vacuum();

	logStart();
	if (m_count_packed)
		logAdd() << nfiles(m_count_packed) << " packed";
	if (m_count_rescanned)
		logAdd() << nfiles(m_count_rescanned) << " rescanned";
	if (m_count_deindexed)
		logAdd() << nfiles(m_count_deindexed) << " removed from index";
	//if (size_vacuum)
		//logAdd() << size_vacuum << " bytes reclaimed cleaning the index";
	logEnd();
}

}
}
}
// vim:set ts=4 sw=4:
