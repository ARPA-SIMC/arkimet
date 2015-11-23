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

#include <wibble/sys/signal.h>
#include <wibble/sys/buffer.h>

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace utils {
namespace ds {

bool DataOnly::eat(unique_ptr<Metadata>&& md)
{
    if (!writer)
        writer = dataset::data::OstreamWriter::get(md->source().format);
    writer->stream(*md, out);
    return true;
}

bool MakeAbsolute::eat(unique_ptr<Metadata>&& md)
{
    if (md->has_source())
        if (const source::Blob* blob = dynamic_cast<const source::Blob*>(&(md->source())))
            md->set_source(upcast<Source>(blob->makeAbsolute()));
    return next.eat(move(md));
}

bool MakeURL::eat(std::unique_ptr<Metadata>&& md)
{
    md->set_source(Source::createURL(md->source().format, url));
    return next.eat(move(md));
}

}
}
}
