#ifndef ARKI_SUMMARY_CODEC_H
#define ARKI_SUMMARY_CODEC_H

/*
 * summary/codec - Summary I/O implementation
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

#include <arki/types.h>
#include <arki/summary.h>
#include <vector>

namespace arki {
class Metadata;
class Matcher;

namespace summary {
struct Stats;
struct Visitor;
struct StatsVisitor;
struct ItemVisitor;
struct Table;

size_t decode(const std::vector<uint8_t>& buf, unsigned version, const std::string& filename, Table& target);

struct EncodingVisitor : public Visitor
{
    // Encoder we send data to
    utils::codec::Encoder& enc;

    // Last metadata encoded so far
    std::vector<const types::Type*> last;

    EncodingVisitor(utils::codec::Encoder& enc);

    bool operator()(const std::vector<const types::Type*>& md, const Stats& stats) override;
};

}
}
#endif
