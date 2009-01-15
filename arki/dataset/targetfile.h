#ifndef ARKI_DATASET_TARGETFILE_H
#define ARKI_DATASET_TARGETFILE_H

/*
 * dataset - Handle xgribarch datasets
 *
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <string>
#include <vector>

namespace arki {

class Metadata;
class ConfigFile;

namespace matcher {
class Implementation;
}

namespace dataset {

/**
 * Generator for filenames the dataset in which to store a metadata
 */
struct TargetFile
{
	virtual ~TargetFile() {}
	virtual std::string operator()(const Metadata& md) = 0;

	/**
	 * Check if a given path (even a partial path) can contain things that
	 * match the given matcher.
	 *
	 * Currently it can only look at the reftime part of the matcher.
	 */
	virtual bool pathMatches(const std::string& path, const matcher::Implementation& m) const = 0;

	/**
	 * Create a TargetFile according to the given configuration.
	 *
	 * Usually, only the 'step' configuration key is considered.
	 */
	static TargetFile* create(const ConfigFile& cfg);

	/**
	 * Return the list of available steps
	 */
	static std::vector<std::string> stepList();
};

}
}

// vim:set ts=4 sw=4:
#endif
