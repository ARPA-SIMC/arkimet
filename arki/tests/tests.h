/**
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
#ifndef ARKI_TEST_UTILS_H
#define ARKI_TEST_UTILS_H

#include <arki/wibble/tests.h>
#include <memory>

namespace arki {
namespace tests {

struct ArkiCheck
{
    virtual ~ArkiCheck() {}
    virtual void check(WIBBLE_TEST_LOCPRM) const = 0;
};

}
}

namespace wibble {
namespace tests {

static inline void _wassert(WIBBLE_TEST_LOCPRM, std::unique_ptr<arki::tests::ArkiCheck> expr)
{
    expr->check(wibble_test_location);
}

}
}

#endif
