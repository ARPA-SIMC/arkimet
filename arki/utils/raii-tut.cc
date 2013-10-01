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

#include <arki/tests/tests.h>

namespace tut {
using namespace std;
using namespace wibble::tests;
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
        wassert(actual(a) == 1);
    }
    wassert(actual(a) == 3);

    std::string b = "foo";
    {
        SetOnExit<std::string> zz(b, "bar");
        wassert(actual(b) == "foo");
    }
    wassert(actual(b) == "bar");
}

// Check DeleteAndZeroOnExit
template<> template<>
void to::test<2>()
{
    int *a = new int(1);
    {
        DeleteAndZeroOnExit<int> zz(a);
        wassert(actual(*a) == 1);
    }
    wassert(actual(a) == (int*)0);
}

// Check DeleteArrayAndZeroOnExit
template<> template<>
void to::test<3>()
{
    int *a = new int[10];
    a[0] = 1;
    {
        DeleteArrayAndZeroOnExit<int> zz(a);
        wassert(actual(a[0]) == 1);
    }
    wassert(actual(a) == (int*)0);
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
        wassert(actual(a) == (int*)0);
    }

    // Test commit
    {
        int *a = (int*)1;
        {
            TransactionAllocArray<int> ta(a, 10);
            a[0] = 42;
            ta.commit();
        }
        wassert(actual(a[0]) == 42);
        delete[] a;
    }

    // Test alloc and initialize
    {
        char *a = (char*)1;
        {
            TransactionAllocArray<char> ta(a, 7, "antani");
            ta.commit();
        }
        wassert(actual(a) == "antani");
        delete[] a;
    }
}

}

// vim:set ts=4 sw=4:
