/**
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/metadata.h>

namespace arki {
namespace tests {

/// Check that the two metadata are the same, except for source and notes
void test_assert_md_similar(ARKI_TEST_LOCPRM, const Metadata& expected, const Metadata& actual);

/// Check that the metadata contains a given item
void test_assert_md_contains(ARKI_TEST_LOCPRM, const std::string& type, const std::string& expected_val, const Metadata& actual);

/// Check that the metadata does not contain
void test_assert_md_unset(ARKI_TEST_LOCPRM, const std::string& type, const Metadata& actual);

}
}

#endif
