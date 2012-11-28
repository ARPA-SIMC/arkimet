/*
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

#include "config.h"

#include <arki/tests/test-utils.h>
#include "arki/data/impl.h"

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::data;
using namespace wibble;

struct arki_data_impl_shar {
};

TESTGRP(arki_data_impl);

struct Noop : public Transaction
{
    virtual void commit() {}
    virtual void rollback() {}
};

class TestWriter : public impl::Writer
{
public:
    TestWriter(const std::string& fname)
        : impl::Writer(fname) {}

    virtual void append(Metadata& md) {}
    virtual Pending append(Metadata& md, off_t* ofs)
    {
        *ofs = 0;
        return Pending(new Noop);
    }
};

template<> template<>
void to::test<1>()
{
    impl::Registry<impl::Writer>& reg = impl::Writer::registry();

    // At first, there is no such entry in the registry
    atest(equals, (void*)0, reg.get("foo"));

    // Create and add it
    impl::Writer* w = reg.add(new TestWriter("foo"));
    w->ref();

    // Now it is in the registry
    atest(equals, w, reg.get("foo"));

    // Lose the last reference
    w->unref();

    // Also removed from registry
    atest(equals, (void*)0, reg.get("foo"));
}

}