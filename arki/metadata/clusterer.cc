/*
 * metadata/clusterer - Process a stream of metadata in batches
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */
#include "clusterer.h"
#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <cstring>

using namespace std;
using namespace arki;
using namespace wibble;

namespace arki {
namespace metadata {

Clusterer::Clusterer()
    : max_count(0), max_bytes(0), max_interval(0), split_timerange(false)
{
    cur_interval[0] = -1;
}

Clusterer::~Clusterer()
{
    if (!format.empty())
        flush();
}

bool Clusterer::exceeds_count(Metadata& md) const
{
    return (max_count != 0 && count >= max_count);
}

bool Clusterer::exceeds_size(sys::Buffer& buf) const
{
    if (max_bytes == 0 || size == 0) return false;
    return size + buf.size() > max_bytes;
}

bool Clusterer::exceeds_interval(Metadata& md) const
{
    if (max_interval == 0) return false;
    if (cur_interval[0] == -1) return false;
    int candidate[6];
    md_to_interval(md, candidate);
    return memcmp(cur_interval, candidate, 6*sizeof(int)) != 0;
}

bool Clusterer::exceeds_timerange(Metadata& md) const
{
    if (not split_timerange) return false;
    if (not last_timerange.defined()) return false;
    if (last_timerange == md.get<types::Timerange>()) return false;
    return true;
}

void Clusterer::add_to_batch(Metadata& md, sys::Buffer& buf)
{
    size += buf.size();
    ++count;
    if (cur_interval[0] == -1 && max_interval != 0)
        md_to_interval(md, cur_interval);
    timespan.merge(md.get(types::TYPE_REFTIME).upcast<types::Reftime>());
    if (split_timerange and not last_timerange.defined())
        last_timerange = md.get<types::Timerange>();
}

void Clusterer::start_batch(const std::string& new_format)
{
    format = new_format;
    count = 0;
    size = 0;
}

void Clusterer::flush()
{
    // Reset the information about the current cluster
    format.clear();
    count = 0;
    size = 0;
    cur_interval[0] = -1;
    timespan.clear();
    if (split_timerange)
        last_timerange.clear();
}

bool Clusterer::operator()(Metadata& md)
{
    sys::Buffer buf = md.getData();

    if (format.empty() || format != md.source->format ||
        exceeds_count(md) || exceeds_size(buf) || exceeds_interval(md) || exceeds_timerange(md))
    {
        flush();
        start_batch(md.source->format);
    }

    add_to_batch(md, buf);

    return true;
}

void Clusterer::md_to_interval(Metadata& md, int* interval) const
{
    UItem<types::Reftime> rt = md.get(types::TYPE_REFTIME).upcast<types::Reftime>();
    if (!rt.defined())
        throw wibble::exception::Consistency("computing time interval", "metadata has no reference time");
    UItem<types::Time> t;
    switch (rt->style())
    {
        case types::Reftime::POSITION: t = rt.upcast<types::reftime::Position>()->time; break;
        case types::Reftime::PERIOD: t = rt.upcast<types::reftime::Period>()->end; break;
        default:
            throw wibble::exception::Consistency("computing time interval", "reference time has invalid style: " + types::Reftime::formatStyle(rt->style()));
    }
    for (unsigned i = 0; i < 6; ++i)
        interval[i] = i < max_interval ? t->vals[i] : -1;
}


}
}

// vim:set ts=4 sw=4:
