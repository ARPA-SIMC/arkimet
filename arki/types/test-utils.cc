/**
 * Copyright (C) 2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/test-utils.h>
#include <arki/types/source.h>
#include <wibble/sys/fs.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace wibble;

namespace arki {
namespace tests {

void test_assert_sourceblob_is(LOCPRM,
        const std::string& format,
        const std::string& basedir,
        const std::string& fname,
        uint64_t ofs,
        uint64_t size,
        const arki::Item<>& item)
{
    using namespace wibble;

    arki::Item<types::source::Blob> blob = item.upcast<types::source::Blob>();
    if (!blob.defined())
    {
        std::stringstream ss;
        ss << "metadata item '" << blob << "' is not a source::Blob";
        loc.fail_test(ss.str());
    }

    iatest(equals, format, blob->format);
    iatest(equals, basedir, blob->basedir);
    iatest(equals, fname, blob->filename);
    iatest(equals, ofs, blob->offset);
    iatest(equals, size, blob->size);
    iatest(equals, blob->absolutePathname(), sys::fs::abspath(str::joinpath(basedir, fname)));
}

}
}
