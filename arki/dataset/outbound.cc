/*
 * dataset/outbound - Local, non queryable, on disk dataset
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/outbound.h>
#include <arki/dataset/targetfile.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/types/assigneddataset.h>
#include <arki/data.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <fstream>
#include <sstream>
#include <sys/stat.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {

Outbound::Outbound(const ConfigFile& cfg) : m_tf(0)
{
	m_name = cfg.value("name");
	m_path = cfg.value("path");
	m_tf = TargetFile::create(cfg);
}

Outbound::~Outbound()
{
	if (m_tf) delete m_tf;
}

void Outbound::storeBlob(Metadata& md, const std::string& reldest)
{
    // Read source information
    UItem<types::Source> s = md.source;

    // Write using data::Writer
    string datafilename = str::joinpath(m_path, reldest) + "." + s->format;
    data::Writer w = data::Writer::get(s->format, datafilename);
    w.append(md);
}

WritableDataset::AcquireResult Outbound::acquire(Metadata& md, ReplaceStrategy replace)
{
	string reldest = (*m_tf)(md);
	string dest = m_path + "/" + reldest;

	wibble::sys::fs::mkFilePath(dest);

	md.set(types::AssignedDataset::create(m_name, ""));

	try {
		storeBlob(md, reldest);
		return ACQ_OK;
	} catch (std::exception& e) {
		md.add_note(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
		return ACQ_ERROR;
	}

	// This should never be reached, but we throw an exception to avoid a
	// warning from the compiler
	throw wibble::exception::Consistency("this code is here to appease a compiler warning", "this code path should never be reached");
}

void Outbound::remove(Metadata&)
{
    throw wibble::exception::Consistency("removing data from outbound dataset", "dataset does not support removing items");
}

void Outbound::removeAll(std::ostream& log, bool writable)
{
	log << m_name << ": cleaning dataset not implemented" << endl;
}

WritableDataset::AcquireResult Outbound::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	auto_ptr<TargetFile> tf(TargetFile::create(cfg));
	string dest = cfg.value("path") + "/" + (*tf)(md) + "." + md.source->format;
	out << "Assigning to dataset " << cfg.value("name") << " in file " << dest << endl;
	return ACQ_OK;
}

}
}
// vim:set ts=4 sw=4:
