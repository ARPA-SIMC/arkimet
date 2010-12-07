/*
 * dataset/simple/reader - Reader for simple datasets with no duplicate checks
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

#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/index.h>
#include <arki/configfile.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/metadata/collection.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/utils/compress.h>
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

Reader::Reader(const ConfigFile& cfg)
	: Local(cfg), m_mft(0)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_path);
	
	if (Manifest::exists(m_path))
	{
		auto_ptr<Manifest> mft = Manifest::create(m_path);

		m_mft = mft.release();
		m_mft->openRO();
	}
}

Reader::~Reader()
{
	if (m_mft) delete m_mft;
}

bool Reader::is_dataset(const std::string& dir)
{
	return Manifest::exists(dir);
}

void Reader::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	Local::queryData(q, consumer);
	if (!m_mft) return;
	m_mft->queryData(q, consumer);
}

void Reader::querySummary(const Matcher& matcher, Summary& summary)
{
	Local::querySummary(matcher, summary);
	if (!m_mft) return;
	m_mft->querySummary(matcher, summary);
}

size_t Reader::produce_nth(metadata::Consumer& cons, size_t idx)
{
    size_t res = Local::produce_nth(cons, idx);
    if (m_mft)
        res += m_mft->produce_nth(cons, idx);
    return res;
}

void Reader::maintenance(maintenance::MaintFileVisitor& v)
{
	if (!m_mft) return;
	m_mft->check(v);
}

}
}
}
