#ifndef ARKI_UTILS_DATASET_H
#define ARKI_UTILS_DATASET_H

/*
 * utils/dataset - Useful functions for working with datasets
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

namespace dataset {
namespace data {
struct OstreamWriter;
}
}

namespace utils {
namespace ds {

struct DataStartHookRunner : public metadata::Eater
{
    metadata::Eater& next;
    metadata::Hook* data_start_hook;

    DataStartHookRunner(metadata::Eater& next, metadata::Hook* data_start_hook=0)
        : next(next), data_start_hook(data_start_hook) {}

    bool eat(std::auto_ptr<Metadata> md) override
    {
        if (data_start_hook)
        {
            (*data_start_hook)();
            data_start_hook = 0;
        }
        return next.eat(md);
    }
};

/**
 * Output the data from a metadata stream into an ostream
 */
struct DataOnly : public metadata::Eater
{
    std::ostream& out;
    const dataset::data::OstreamWriter* writer;

    DataOnly(std::ostream& out) : out(out), writer(0) {}

    bool eat(std::auto_ptr<Metadata> md) override;
};


/**
 * Make all source blobs absolute
 */
struct MakeAbsolute : public metadata::Eater
{
    metadata::Eater& next;

    MakeAbsolute(metadata::Eater& next) : next(next) {}

    bool eat(std::auto_ptr<Metadata> md) override;
};

}
}
}

// vim:set ts=4 sw=4:
#endif
