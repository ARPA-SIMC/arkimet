/*
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include "config.h"

#include <arki/tests/test-utils.h>
#include <arki/utils/pcounter.h>
using namespace arki;

#include <sstream>
#include <fstream>
#include <iostream>
using namespace std;

namespace tut
{

/*============================================================================*/

struct arki_utils_pcounter_shar
{
};

TESTGRP(arki_utils_pcounter);

// Test running one shot insert queries
template<> template<> void to::test<1>()
{
	{
	arki::utils::PersistentCounter<int> c("pcounter_int.txt");
	ensure(c.get() == 0);
	ensure(c.inc() == 1);
	ensure(c.inc() == 2);
	ensure(c.inc() == 3);
	}

	{
	arki::utils::PersistentCounter<int> c;
	c.bind("pcounter_int.txt");
	ensure(c.get() == 3);
	c.unbind();
	}
}

// Test running one shot insert queries
template<> template<> void to::test<2>()
{
	{
	arki::utils::PersistentCounter<double> c("pcounter_double.txt");
	ensure(c.get() == 0);
	ensure(c.inc() == 1);
	ensure(c.inc() == 2);
	ensure(c.inc() == 3);
	}

	{
	arki::utils::PersistentCounter<double> c;
	c.bind("pcounter_double.txt");
	ensure(c.get() == 3);
	c.unbind();
	}
}

/*============================================================================*/

}


























