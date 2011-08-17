/*
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils/accounting.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct arki_utils_accounting_shar {
	utils::acct::Counter counter;
	
	arki_utils_accounting_shar() : counter("foo") {}
};
TESTGRP(arki_utils_accounting);

// Simple counter test
template<> template<>
void to::test<1>()
{
	ensure_equals(string(counter.name()), "foo");
	ensure_equals(counter.val(), 0u);
	counter.incr();
	ensure_equals(counter.val(), 1u);
	counter.incr(10);
	ensure_equals(counter.val(), 11u);
	counter.reset();
	ensure_equals(counter.val(), 0u);
}

}

// vim:set ts=4 sw=4:
