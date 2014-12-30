/*
 * Copyright (C) 2007--2011  Guido Billi <guidobilli@gmail.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include "config.h"

#include <arki/matcher/tests.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/quantity.h>
#include <arki/configfile.h>

#include <sstream>
#include <iostream>

namespace tut {

/*============================================================================*/

using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_matcher_quantity_shar
{
    Metadata md;

    arki_matcher_quantity_shar()
    {
        arki::tests::fill(md);
    }
};

TESTGRP(arki_matcher_quantity);

template<> template<> void to::test<1>()
{
	Matcher m;

	ensure_matches("quantity:", md);
	ensure_matches("quantity:a", md);
	ensure_matches("quantity:b", md);
	ensure_matches("quantity:c", md);
	ensure_matches("quantity:a,b", md);
	ensure_matches("quantity:a,b,c", md);
}

template<> template<> void to::test<2>()
{
	Matcher m;

	ensure_not_matches("quantity:x", md);
	ensure_not_matches("quantity:a,b,c,d", md);
}

/*============================================================================*/

}

// vim:set ts=4 sw=4:



























