/*
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/configfile.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_configfile_shar {
};
TESTGRP(arki_configfile);

// Check that simple key = val items are parsed correctly
template<> template<>
void to::test<1>()
{
	string test =
		"\n" // Empty line
		"#\n" // Empty comment
		"# Comment at the beginning\n"
		"   # Comment after some spaces\n"
		"; Comment at the beginning\n"
		"   ; Comment after some spaces\n"
		"zero = 0\n"
		" uno = 1  \n"
		"due=2\n"
		"  t r e  = 3\n"
		"\n";
	stringstream in(test);

	ConfigFile conf;
	conf.parse(in, "(memory)");

	size_t count = 0;
	for (ConfigFile::const_iterator i = conf.begin(); i != conf.end(); ++i)
		++count;
	ensure_equals(count, 4u);

	ensure_equals(conf.value("zero"), "0");
	const ConfigFile::FilePos* info = conf.valueInfo("zero");
	ensure(info != 0);
	ensure_equals(info->pathname, "(memory)");
	ensure_equals(info->lineno, 7u);

	ensure_equals(conf.value("uno"), "1");
	info = conf.valueInfo("uno");
	ensure(info != 0);
	ensure_equals(info->pathname, "(memory)");
	ensure_equals(info->lineno, 8u);

	ensure_equals(conf.value("due"), "2");
	info = conf.valueInfo("due");
	ensure(info != 0);
	ensure_equals(info->pathname, "(memory)");
	ensure_equals(info->lineno, 9u);

	ensure_equals(conf.value("t r e"), "3");
	info = conf.valueInfo("t r e");
	ensure(info != 0);
	ensure_equals(info->pathname, "(memory)");
	ensure_equals(info->lineno, 10u);

	conf.setValue("due", "DUE");
	ensure_equals(conf.value("due"), "DUE");
	ensure_equals(conf.valueInfo("due"), (void*)NULL);
}

}

// vim:set ts=4 sw=4:
