/*
 * dataset/ondisk2/reader - Local on disk dataset reader
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

#include <arki/dataset/ondisk2/reader.h>
#include <arki/dataset/index/contents.h>
#include <arki/dataset/targetfile.h>
#include <arki/dataset/archive.h>
#include <arki/types/assigneddataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/summary.h>
#include <arki/postprocess.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <fstream>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

#ifdef HAVE_LUA
#include <arki/report.h>
#endif


using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Reader::Reader(const ConfigFile& cfg)
       	: Local(cfg), m_idx(0), m_tf(0)
{
	this->cfg = cfg.values();
	m_tf = TargetFile::create(cfg);
	if (sys::fs::access(str::joinpath(m_path, "index.sqlite"), F_OK))
	{
        m_idx = new index::RContents(cfg);
        m_idx->open();
	}
}

Reader::~Reader()
{
	if (m_idx) delete m_idx;
	if (m_tf) delete m_tf;
}

void Reader::queryLocalData(const dataset::DataQuery& q, metadata::Eater& consumer)
{
    if (!m_idx || !m_idx->query(q, consumer))
        throw wibble::exception::Consistency("querying " + m_path, "index could not be used");
}

void Reader::queryData(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	// Query the archives first
	Local::queryData(q, consumer);

	if (!m_idx) return;

	queryLocalData(q, consumer);
}

void Reader::querySummary(const Matcher& matcher, Summary& summary)
{
	// Query the archives first
	Local::querySummary(matcher, summary);

	if (!m_idx) return;

	using namespace wibble::str;

	if (!m_idx || !m_idx->querySummary(matcher, summary))
		throw wibble::exception::Consistency("querying " + m_path, "index could not be used");
}

size_t Reader::produce_nth(metadata::Eater& cons, size_t idx)
{
    size_t res = Local::produce_nth(cons, idx);
    if (m_idx)
    {
        //ds::MakeAbsolute mkabs(cons);
        res += m_idx->produce_nth(cons, idx);
    }
    return res;
}

}
}
}
// vim:set ts=4 sw=4:
