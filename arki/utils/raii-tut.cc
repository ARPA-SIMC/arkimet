/*
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

#include "raii.h"

#include <arki/tests/test-utils.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils::raii;

struct arki_utils_raii_shar {
};
TESTGRP(arki_utils_raii);

// Check SetOnOexit
template<> template<>
void to::test<1>()
{
    int a = 1;
    {
        SetOnExit<int> zz(a, 3);
        wtest(equals, 1, a);
    }
    wtest(equals, 3, a);

    std::string b = "foo";
    {
        SetOnExit<std::string> zz(b, "bar");
        wtest(equals, "foo", b);
    }
    wtest(equals, "bar", b);
}

// Check DeleteAndZeroOnExit
template<> template<>
void to::test<2>()
{
    int *a = new int(1);
    {
        DeleteAndZeroOnExit<int> zz(a);
        wtest(equals, 1, *a);
    }
    wtest(equals, (int*)0, a);
}

// Check DeleteArrayAndZeroOnExit
template<> template<>
void to::test<3>()
{
    int *a = new int[10];
    a[0] = 1;
    {
        DeleteArrayAndZeroOnExit<int> zz(a);
        wtest(equals, 1, a[0]);
    }
    wtest(equals, (int*)0, a);
}

// Check TransactionAlloc
template<> template<>
void to::test<4>()
{
    // Test rollback
    {
        int *a = (int*)1;
        {
            TransactionAllocArray<int> ta(a, 10);
            a[0] = 42;
            // Do not commit
        }
        wtest(equals, (int*)0, a);
    }

    // Test commit
    {
        int *a = (int*)1;
        {
            TransactionAllocArray<int> ta(a, 10);
            a[0] = 42;
            ta.commit();
        }
        wtest(equals, 42, a[0]);
        delete[] a;
    }

    // Test alloc and initialize
    {
        char *a = (char*)1;
        {
            TransactionAllocArray<char> ta(a, 7, "antani");
            ta.commit();
        }
        wtest(equals, string("antani"), string(a));
        delete[] a;
    }
}

}

// vim:set ts=4 sw=4:
