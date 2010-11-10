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

#include <arki/metadata/consumer.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <iosfwd>
#include <signal.h>

namespace arki {
struct Metadata;

namespace utils {
namespace ds {

/**
 * Prepend a path to all metadata
 */
struct PathPrepender : public metadata::Consumer
{
	std::string path;
	metadata::Consumer& next;
	PathPrepender(const std::string& path, metadata::Consumer& next) : path(path), next(next) {}
	bool operator()(Metadata& md)
	{
		md.prependPath(path);
		return next(md);
	}
};

struct MatcherFilter : public metadata::Consumer
{
	const Matcher& matcher;
	metadata::Consumer& next;
	MatcherFilter(const Matcher& matcher, metadata::Consumer& next)
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
struct DataInliner : public metadata::Consumer
{
	metadata::Consumer& next;
	DataInliner(metadata::Consumer& next) : next(next) {}
	bool operator()(Metadata& md);
};

/**
 * Inline the data into all metadata, but after the next consumer has finished
 * it restores the previous source, deleting the cached data.
 */
struct TemporaryDataInliner : public metadata::Consumer
{
	metadata::Consumer& next;
	TemporaryDataInliner(metadata::Consumer& next) : next(next) {}
	bool operator()(Metadata& md);
};

struct DataStartHookRunner : public metadata::Consumer
{
    metadata::Consumer& next;
    metadata::Hook* data_start_hook;

    DataStartHookRunner(metadata::Consumer& next, metadata::Hook* data_start_hook=0)
        : next(next), data_start_hook(data_start_hook) {}

    bool operator()(Metadata& md)
    {
        if (data_start_hook)
        {
            (*data_start_hook)();
            data_start_hook = 0;
        }
        return next(md);
    }
};

/**
 * Output the data from a metadata stream into an ostream
 */
struct DataOnly : public metadata::Consumer
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
