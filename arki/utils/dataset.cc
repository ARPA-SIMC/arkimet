/*
 * utils/dataset - Useful functions for working with datasets
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "arki/utils/dataset.h"
#include "arki/dataset/data.h"

#include <wibble/sys/signal.h>
#include <wibble/sys/buffer.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace ds {

bool DataInliner::operator()(Metadata& md)
{
    // Read the data and change the source to inline
    md.makeInline();
    return next(md);
}

bool TemporaryDataInliner::operator()(Metadata& md)
{
    // If we get data already inlined, we can shortcut
    if (md.source->style() == types::Source::INLINE)
        return next(md);

    // Save the old source
    UItem<types::Source> oldSource = md.source;

    // Change the source as inline
    md.makeInline();

    // Pass it all to the next step in the processing chain
    bool res = next(md);

    // Drop the cached metadata to avoid keeping all query results in memory
    oldSource->dropCachedData();

    // Restore the old source
    md.source = oldSource;

    return res;
}

bool DataOnly::operator()(Metadata& md)
{
    if (!writer)
        writer = dataset::data::OstreamWriter::get(md.source->format);
    writer->stream(md, out);
    return true;
}

bool MakeAbsolute::operator()(Metadata& md)
{
   Item<types::source::Blob> i = md.source.upcast<types::source::Blob>();
   if (i.defined())
       md.source = i->makeAbsolute();
   return next(md);
}

}
}
}
// vim:set ts=4 sw=4:
