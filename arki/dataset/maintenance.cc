/*
 * dataset/maintenance - Dataset maintenance utilities
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/maintenance.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/metadata.h>
#include <arki/utils/compress.h>
#include <arki/scan/any.h>
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

namespace arki {
namespace dataset {
namespace maintenance {

HoleFinder::HoleFinder(MaintFileVisitor& next, const std::string& root, bool quick)
	: next(next), m_root(root), quick(quick), validator(0), validator_fd(-1),
	  has_hole(false), is_corrupted(false)
{
}

void HoleFinder::finaliseFile()
{
	if (!last_file.empty())
	{
		if (validator_fd >= 0)
		{
			close(validator_fd);
			validator_fd = -1;
		}

		if (is_corrupted)
		{
			nag::verbose("HoleFinder: %s found corrupted", last_file.c_str());
			next(last_file, MaintFileVisitor::TO_RESCAN);
			return;
		}

		off_t size = compress::filesize(str::joinpath(m_root, last_file));
		if (size < last_file_size)
		{
			nag::verbose("HoleFinder: %s found truncated (%zd < %zd bytes)", last_file.c_str(), size, last_file_size);
			// throw wibble::exception::Consistency("checking size of "+last_file, "file is shorter than what the index believes: please run a dataset check");
			next(last_file, MaintFileVisitor::TO_RESCAN);
			return;
		}

		// Check if last_file_size matches the file size
		if (size > last_file_size)
			has_hole = true;

		// Take note of files with holes
		if (has_hole)
		{
			nag::verbose("HoleFinder: %s contains deleted data", last_file.c_str());
			next(last_file, MaintFileVisitor::TO_PACK);
		} else {
			next(last_file, MaintFileVisitor::OK);
		}
	}
}

void HoleFinder::operator()(const std::string& file, int id, off_t offset, size_t size)
{
	if (last_file != file)
	{
		finaliseFile();
		last_file = file;
		last_file_size = 0;
		has_hole = false;
		is_corrupted = false;
		if (!quick)
		{
			string fname = str::joinpath(m_root, file);

			// If the data file is compressed, create a temporary uncompressed copy
			auto_ptr<utils::compress::TempUnzip> tu;
			if (!sys::fs::access(fname, F_OK) && sys::fs::access(fname + ".gz", F_OK))
				tu.reset(new utils::compress::TempUnzip(fname));
			
			validator = &scan::Validator::by_filename(fname.c_str());
			validator_fd = open(fname.c_str(), O_RDONLY);
			if (validator_fd < 0)
			{
				perror(file.c_str());
				is_corrupted = true;
			} else
				(void)posix_fadvise(validator_fd, 0, 0, POSIX_FADV_DONTNEED);
		}
	}
	// If we've already found that the file is corrupted, there is nothing
	// else to do
	if (is_corrupted) return;
	if (!quick)
		try {
			validator->validate(validator_fd, offset, size, file);
		} catch (wibble::exception::Generic& e) {
			// FIXME: we do not have a better interface to report
			// error strings, so we fall back to cerr. It will be
			// useless if we are hidden behind a graphical
			// interface, but in all other cases it's better than
			// nothing. HOWEVER, printing to stderr creates noise
			// during the unit tests.
			string fname = str::joinpath(m_root, file);
			nag::warning("corruption detected at %s:%ld-%zd: %s", fname.c_str(), offset, size, e.desc().c_str());
			is_corrupted = true;
		}

	if (offset != last_file_size)
		has_hole = true;
	last_file_size += size;
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

void FindMissing::operator()(const std::string& file, State state)
{
	while (not disk.empty() and disk.back() < file)
	{
		nag::verbose("FindMissing: %s is not in index", disk.back().c_str());
		next(disk.back(), TO_INDEX);
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
		next(file, DELETED);
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

void MaintPrinter::operator()(const std::string& file, State state)
{
	switch (state)
	{
		case OK: cerr << file << " OK" << endl;
		case TO_ARCHIVE: cerr << file << " TO ARCHIVE" << endl;
		case TO_DELETE: cerr << file << " TO DELETE" << endl;
		case TO_PACK: cerr << file << " TO PACK" << endl;
		case TO_INDEX: cerr << file << " TO INDEX" << endl;
		case TO_RESCAN: cerr << file << " TO RESCAN" << endl;
		case DELETED: cerr << file << " DELETED" << endl;
		case ARC_OK: cerr << file << " ARCHIVED OK" << endl;
		case ARC_TO_INDEX: cerr << file << " TO INDEX IN ARCHIVE" << endl;
		case ARC_TO_RESCAN: cerr << file << " TO RESCAN IN ARCHIVE" << endl;
		case ARC_DELETED: cerr << " DELETED IN ARCHIVE" << endl;
		default: cerr << " INVALID" << endl;
	}
}

}
}
}
// vim:set ts=4 sw=4:
