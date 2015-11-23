/**
 * Copyright (C) 2012--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#ifndef ARKI_METADATA_TESTS_H
#define ARKI_METADATA_TESTS_H

#include <arki/types/tests.h>
#include <arki/metadata.h>

namespace arki {
class Metadata;

namespace tests {

struct ActualMetadata : public wibble::tests::Actual<Metadata>
{
    ActualMetadata(const Metadata& s) : Actual<Metadata>(s) {}

    /// Check that a metadata field has the expected value
    std::unique_ptr<ArkiCheck> contains(const std::string& field, const std::string& expected);

    /// Check that the two metadata are the same, except for source and notes
    std::unique_ptr<ArkiCheck> is_similar(const Metadata& expected);

    /// Check that the metadata does contain an item of the given type
    std::unique_ptr<ArkiCheck> is_set(const std::string& field);

    /// Check that the metadata does contain an item of the given type
    std::unique_ptr<ArkiCheck> is_not_set(const std::string& field);
};

}
}

namespace wibble {
namespace tests {

inline arki::tests::ActualMetadata actual(const arki::Metadata& actual) { return arki::tests::ActualMetadata(actual); }

}
}

#endif
