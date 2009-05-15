/*
 * dataset - Handle arkimet datasets
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
#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/dataset/file.h>
#include <arki/dataset/ondisk.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/outbound.h>
#include <arki/dataset/discard.h>
#include <arki/dataset/empty.h>
#include <arki/metadata.h>
#include <arki/types/assigneddataset.h>
#include <arki/utils.h>

#include <wibble/exception.h>
#include <wibble/string.h>

#ifdef HAVE_LIBCURL
#include <arki/dataset/http.h>
#endif

using namespace std;
using namespace wibble;

namespace arki {

void WritableDataset::flush() {}

void WritableDataset::repack(std::ostream& log, bool writable) {}
void WritableDataset::check(std::ostream& log) {}
void WritableDataset::check(std::ostream& log, MetadataConsumer& salvage) {}

void WritableDataset::remove(Metadata& md)
{
	Item<types::AssignedDataset> ds = md.get(types::TYPE_ASSIGNEDDATASET).upcast<types::AssignedDataset>();
	if (!ds.defined())
		throw wibble::exception::Consistency("removing metadata from dataset", "the metadata is not assigned to this dataset");

	remove(ds->id);

	// reset source and dataset in the metadata
	md.source.clear();
	md.unset(types::TYPE_ASSIGNEDDATASET);
}

ReadonlyDataset* ReadonlyDataset::create(const ConfigFile& cfg)
{
	string type = wibble::str::tolower(cfg.value("type"));
	if (type.empty())
		type = "local";
	
	if (type == "local" || type == "test" || type == "error" || type == "duplicates")
		return new dataset::ondisk::Reader(cfg);
	if (type == "ondisk2")
		return new dataset::ondisk2::Reader(cfg);
#ifdef HAVE_LIBCURL
	if (type == "remote")
		return new dataset::HTTP(cfg);
#endif
	if (type == "outbound")
		return new dataset::Empty(cfg);
	if (type == "discard")
		return new dataset::Empty(cfg);
	if (type == "file")
		return dataset::File::create(cfg);

	throw wibble::exception::Consistency("creating a dataset", "unknown dataset type \""+type+"\"");
}

void ReadonlyDataset::readConfig(const std::string& path, ConfigFile& cfg)
{
#ifdef HAVE_LIBCURL
	if (str::startsWith(path, "http://") || str::startsWith(path, "https://"))
	{
		return dataset::HTTP::readConfig(path, cfg);
	} else
#endif
	if (utils::isdir(path))
		return dataset::Local::readConfig(path, cfg);
	else
		return dataset::File::readConfig(path, cfg);
}

WritableDataset* WritableDataset::create(const ConfigFile& cfg)
{
	string type = wibble::str::tolower(cfg.value("type"));
	if (type.empty())
		type = "local";
	
	if (type == "local" || type == "test" || type == "error" || type == "duplicates")
		return new dataset::ondisk::Writer(cfg);
	if (type == "ondisk2")
		return new dataset::ondisk2::Writer(cfg);
	if (type == "remote")
		throw wibble::exception::Consistency("creating a dataset", "remote datasets are not writable");
	if (type == "outbound")
		return new dataset::Outbound(cfg);
	if (type == "discard")
		return new dataset::Discard(cfg);

	throw wibble::exception::Consistency("creating a dataset", "unknown dataset type \""+type+"\"");
}

WritableDataset::AcquireResult WritableDataset::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	string type = wibble::str::tolower(cfg.value("type"));
	if (type.empty())
		type = "local";
	
	if (type == "local" || type == "test" || type == "error" || type == "duplicates")
		return dataset::ondisk::Writer::testAcquire(cfg, md, out);
	if (type == "ondisk2")
		return dataset::ondisk2::Writer::testAcquire(cfg, md, out);
	if (type == "remote")
		throw wibble::exception::Consistency("simulating dataset acquisition", "remote datasets are not writable");
	if (type == "outbound")
		return dataset::Outbound::testAcquire(cfg, md, out);
	if (type == "discard")
		return dataset::Discard::testAcquire(cfg, md, out);

	throw wibble::exception::Consistency("simulating dataset acquisition", "unknown dataset type \""+type+"\"");
}

}
// vim:set ts=4 sw=4:
