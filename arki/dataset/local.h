#ifndef ARKI_DATASET_LOCAL_H
#define ARKI_DATASET_LOCAL_H

/*
 * dataset/local - Base class for local datasets
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset.h>
#include <string>

namespace arki {

class ConfigFile;
class Metadata;
class MetadataConsumer;
class Matcher;

namespace dataset {

/**
 * Base class for local datasets
 */
class Local : public ReadonlyDataset
{
public:
	static void readConfig(const std::string& path, ConfigFile& cfg);
};

}
}

// vim:set ts=4 sw=4:
#endif
