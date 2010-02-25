#ifndef ARKI_QUERYMACRO_H
#define ARKI_QUERYMACRO_H

/*
 * arki/querymacro - Macros implementing special query strategies
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/utils/lua.h>
#include <arki/dataset.h>
#include <string>
#include <map>

namespace arki {
class ConfigFile;
class Metadata;
class ReadonlyDataset;

class Querymacro : public ReadonlyDataset
{
protected:
	const ConfigFile& cfg;
	Lua *L;
	std::map<std::string, ReadonlyDataset*> ds_cache;
	// std::map<std::string, int> ref_cache;

	ReadonlyDataset* dataset(const std::string& name);

public:
	/**
	 * Create a query macro read from the query macro file with the given
	 * name.
	 *
	 * @param cfg
	 *   Configuration used to instantiate datasets
	 */
	Querymacro(const ConfigFile& cfg, const std::string& name, const std::string& data);
	virtual ~Querymacro();

	virtual void queryData(const dataset::DataQuery& q, MetadataConsumer& consumer);
	virtual void querySummary(const Matcher& matcher, Summary& summary);
#if 0
	/**
	 * Get a target file generator
	 *
	 * @param def
	 *   Target file definition in the form "type:parms".
	 */
	Func get(const std::string& def);
#endif
};

}

// vim:set ts=4 sw=4:
#endif
