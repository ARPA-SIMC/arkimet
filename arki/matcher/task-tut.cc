
/*
 * Copyright (C) 2007,2010  Guido Billi <guidobilli@gmail.com>
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

#include <arki/matcher/test-utils.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/task.h>
#include <arki/configfile.h>

#include <sstream>
#include <iostream>

namespace tut {

/*============================================================================*/

using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_matcher_task_shar
{
	Metadata md;

	arki_matcher_task_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};

TESTGRP(arki_matcher_task);

template<> template<> void to::test<1>()
{
	Matcher m;

	ensure_matches("task:", md);
	ensure_matches("task:task1", md);	//match esatto
	ensure_matches("task:TASK1", md);	//match case insensitive
	ensure_matches("task:ASK", md);		//match per sottostringa
}

template<> template<> void to::test<2>()
{
	Matcher m;

	ensure_not_matches("task:baaaaa", md);
}

/*============================================================================*/

}

// vim:set ts=4 sw=4:



























