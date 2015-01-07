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
#include <arki/dataset/data.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <fstream>
#include <sstream>
#include <sys/stat.h>

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace dataset {

Outbound::Outbound(const ConfigFile& cfg)
    : WritableLocal(cfg)
{
}

Outbound::~Outbound()
{
}

void Outbound::storeBlob(Metadata& md, const std::string& reldest)
{
    // Write using data::Writer
    data::Writer* w = file(md, md.source().format);
    w->append(md);
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
        md.add_note(*Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
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

size_t Outbound::repackFile(const std::string& relpath)
{
    throw wibble::exception::Consistency("repacking file " + relpath, "dataset does not support repacking files");
}

void Outbound::rescanFile(const std::string& relpath)
{
    throw wibble::exception::Consistency("rescanning file " + relpath, "dataset does not support rescanning files");
}

size_t Outbound::removeFile(const std::string& relpath, bool withData)
{
    throw wibble::exception::Consistency("removing file " + relpath, "dataset does not support removing files");
}

size_t Outbound::vacuum()
{
    return 0;
}

WritableDataset::AcquireResult Outbound::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    auto_ptr<TargetFile> tf(TargetFile::create(cfg));
    string dest = cfg.value("path") + "/" + (*tf)(md) + "." + md.source().format;
    out << "Assigning to dataset " << cfg.value("name") << " in file " << dest << endl;
    return ACQ_OK;
}

}
}
// vim:set ts=4 sw=4:
