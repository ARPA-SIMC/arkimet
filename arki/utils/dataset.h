#ifndef ARKI_UTILS_DATASET_H
#define ARKI_UTILS_DATASET_H

/*
 * utils/dataset - Useful functions for working with datasets
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

#include <arki/metadata.h>
#include <arki/matcher.h>
#include <iosfwd>
#include <signal.h>

namespace arki {
namespace utils {
namespace ds {

/**
 * Prepend a path to all metadata
 */
struct PathPrepender : public MetadataConsumer
{
	std::string path;
	MetadataConsumer& next;
	PathPrepender(const std::string& path, MetadataConsumer& next) : path(path), next(next) {}
	bool operator()(Metadata& md)
	{
		md.prependPath(path);
		return next(md);
	}
};

struct MatcherFilter : public MetadataConsumer
{
	const Matcher& matcher;
	MetadataConsumer& next;
	MatcherFilter(const Matcher& matcher, MetadataConsumer& next)
		: matcher(matcher), next(next) {}
	bool operator()(Metadata& md)
	{
		if (!matcher(md))
			return true;
		return next(md);
	}
};


/**
 * Inline the data into all metadata
 */
struct DataInliner : public MetadataConsumer
{
	MetadataConsumer& next;
	DataInliner(MetadataConsumer& next) : next(next) {}
	bool operator()(Metadata& md);
};

/**
 * Output the data from a metadata stream into an ostream
 */
struct DataOnly : public MetadataConsumer
{
	std::ostream& out;
	sigset_t blocked;

	DataOnly(std::ostream& out) : out(out)
	{
		sigemptyset(&blocked);
		sigaddset(&blocked, SIGINT);
		sigaddset(&blocked, SIGTERM);
	}
	bool operator()(Metadata& md);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
