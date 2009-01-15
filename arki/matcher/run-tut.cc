/*
 * Copyright (C) 2008  Enrico Zini <enrico@enricozini.org>
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
#include <arki/types/run.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;

struct arki_matcher_run_shar {
	Metadata md;

	arki_matcher_run_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};
TESTGRP(arki_matcher_run);

// Try matching Minute run
template<> template<>
void to::test<1>()
{
	Matcher m;

	m = Matcher::parse("run:MINUTE");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,12");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,12:00");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,13");
	ensure(not m(md));
	m = Matcher::parse("run:MINUTE,12:01");
	ensure(not m(md));

	// Set a different minute
	md.set(run::Minute::create(9, 30));
	m = Matcher::parse("run:MINUTE");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,09:30");
	ensure(m(md));
	m = Matcher::parse("run:MINUTE,09");
	ensure(not m(md));
	m = Matcher::parse("run:MINUTE,09:31");
	ensure(not m(md));
}

}

// vim:set ts=4 sw=4:
