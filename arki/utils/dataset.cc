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

#include "dataset.h"
#include "arki/dataset/data.h"
#include "arki/types/source/blob.h"
#include "arki/data.h"

#include <wibble/sys/signal.h>
#include <wibble/sys/buffer.h>

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace utils {
namespace ds {

bool DataCacher::eat(auto_ptr<Metadata> md)
{
    // Load the data into the cache
    if (md->has_source_blob())
        Data::current().prefetch(md->sourceBlob());
    return next.eat(md);
}

bool TemporaryDataInliner::operator()(Metadata& md)
{
    // If we get data already inlined, we can shortcut
    if (md.source().style() == types::Source::INLINE)
        return next(md);

    // Save the old source
    auto_ptr<types::Source> old_source(md.source().clone());

    // Change the source as inline
    md.makeInline();

    // Pass it all to the next step in the processing chain
    bool res = next(md);

    // Drop the cached metadata to avoid keeping all query results in memory
    // FIXME old_source->dropCachedData();

    // Restore the old source
    md.set_source(old_source);

    return res;
}

bool DataOnly::eat(auto_ptr<Metadata> md)
{
    if (!writer)
        writer = dataset::data::OstreamWriter::get(md->source().format);
    writer->stream(*md, out);
    return true;
}

bool MakeAbsolute::eat(auto_ptr<Metadata> md)
{
    if (md->has_source())
        if (const source::Blob* blob = dynamic_cast<const source::Blob*>(&(md->source())))
            md->set_source(upcast<Source>(blob->makeAbsolute()));
    return next.eat(md);
}

}
}
}
