/*
 * Copyright (C) 2008--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher/test-utils.h>
#include <arki/formatter.h>
#include <arki/metadata.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
    m.writeYaml(o);
	return o;
}
}

namespace tut {
using namespace std;
using namespace arki;

struct arki_formatter_shar {
    Metadata md;

    arki_formatter_shar()
    {
        md.create();
        arki::tests::fill(md);
    }
};
TESTGRP(arki_formatter);

// See if the formatter makes a difference
template<> template<>
void to::test<1>()
{
	auto_ptr<Formatter> formatter(Formatter::create());

	stringstream str1;
	md.writeYaml(str1);

	stringstream str2;
	md.writeYaml(str2, formatter.get());
	
	// They must be different
	ensure(str1.str() != str2.str());

	// str2 contains annotations, so it should be longer
	ensure(str1.str().size() < str2.str().size());

	// Read back the two metadatas
	Metadata md1; md1.create();
	{
		stringstream str(str1.str(), ios_base::in);
		md1.readYaml(str, "(test memory buffer)");
	}
	Metadata md2; md2.create();
	{
		stringstream str(str2.str(), ios_base::in);
		md2.readYaml(str, "(test memory buffer)");
	}

	// Once reparsed, they should have the same content
	ensure_equals(md, md1);
	ensure_equals(md, md2);
}

}

// vim:set ts=4 sw=4:
