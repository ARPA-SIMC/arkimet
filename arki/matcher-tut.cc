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

#include <arki/matcher/tests.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/utils/lua.h>
#include <arki/runtime/config.h>

#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <fstream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;

struct arki_matcher_shar
{
    Metadata md;

    arki_matcher_shar()
    {
        MatcherAliasDatabase::reset();
        arki::tests::fill(md);
    }
};
TESTGRP(arki_matcher);

// Try OR matches
template<> template<>
void to::test<1>()
{
	Matcher m;

	m = Matcher::parse("origin:GRIB1 OR BUFR");
	ensure(m(md));

	m = Matcher::parse("origin:BUFR or GRIB1");
	ensure(m(md));

	m = Matcher::parse("origin:BUFR or BUFR");
	ensure(!m(md));
}

// Try using aliases
template<> template<>
void to::test<2>()
{
	// Configuration file with alias definitions
	string test =
		"[origin]\n"
		"valid = GRIB1,1,2,3\n"
		"invalid = GRIB1,2\n"
		"[product]\n"
		"valid = GRIB1,1,2,3\n"
		"invalid = GRIB1,2\n"
		"[level]\n"
		"valid = GRIB1,110,12,13\n"
		"invalid = GRIB1,12\n"
		"[timerange]\n"
		"valid = GRIB1,2,22s,23s\n"
		"invalid = GRIB1,22\n"
		"[reftime]\n"
		"valid = >=2007,<=2008\n"
		"invalid = <2007\n";
	stringstream in(test);
	ConfigFile conf;
	conf.parse(in, "(memory)");

	MatcherAliasDatabase::addGlobal(conf);

	ensure_matches("origin:valid", md);
	ensure_not_matches("origin:invalid", md);

	ensure_matches("product:valid", md);
	ensure_not_matches("product:invalid", md);

	ensure_matches("level:valid", md);
	ensure_not_matches("level:invalid", md);

	ensure_matches("timerange:valid", md);
	ensure_not_matches("timerange:invalid", md);

	ensure_matches("reftime:valid", md);
	ensure_not_matches("reftime:invalid", md);
}

// Test toString()
// Kind of pointless now, since it just returns the original unparsed string
template<> template<>
void to::test<3>()
{
	ensure_equals(Matcher::parse("origin:GRIB1,1,,3 or BUFR,1").toString(), "origin:GRIB1,1,,3 or BUFR,1");
	//ensure_equals(Matcher::parse("reftime:>=2007-01-02 03:04").toString(), "reftime:>=2007-01-02 03:04:00");
	//ensure_equals(Matcher::parse("area:GRIB:foo=5,bar=5000,pippo=\"pippo\"").toString(), "area:GRIB:bar=5000, foo=5, pippo=pippo");

	//ensure_equals(Matcher::parse("origin:GRIB1,1,,3 or BUFR,1; reftime:<2007,>=2007-01-02 03:04 or >2008; area:GRIB:foo=5,bar=5000,pippo=\"pippo\"").toString(), "origin:GRIB1,1,,3 or BUFR,1; reftime:<2007-01-01 00:00:00,>=2007-01-02 03:04:00 or >2008-12-31 23:59:59; area:GRIB:bar=5000, foo=5, pippo=pippo");
}

// Aliases that refer to aliases
template<> template<>
void to::test<4>()
{
	// Configuration file with alias definitions
	string test =
		"[origin]\n"
		"c = GRIB1,2,3,4\n"
		"b = GRIB1,1,2,3\n"
		"a = c or b\n";
	stringstream in(test);
	ConfigFile conf;
	conf.parse(in, "(memory)");

	MatcherAliasDatabase::addGlobal(conf);

	Matcher m;

	ensure_matches("origin:a", md);
	ensure_matches("origin:b", md);
	ensure_not_matches("origin:c", md);
}

// Recursive aliases should fail
template<> template<>
void to::test<5>()
{
	string test =
		"[origin]\n"
		"a = a or a\n";
	stringstream in(test);
	ConfigFile conf;
	conf.parse(in, "(memory)");
	try {
		MatcherAliasDatabase::addGlobal(conf);
		ensure(false);
	} catch (std::exception& e) {
		ensure(true);
	}
}

// Recursive aliases should fail
template<> template<>
void to::test<6>()
{
	string test =
		"[origin]\n"
		"a = b\n"
		"b = a\n";
	stringstream in(test);
	ConfigFile conf;
	conf.parse(in, "(memory)");
	try {
		MatcherAliasDatabase::addGlobal(conf);
		ensure(false);
	} catch (std::exception& e) {
		ensure(true);
	}
}

// Recursive aliases should fail
template<> template<>
void to::test<7>()
{
	string test =
		"[origin]\n"
		"a = b\n"
		"b = c\n"
		"c = a\n";
	stringstream in(test);
	ConfigFile conf;
	conf.parse(in, "(memory)");
	try {
		MatcherAliasDatabase::addGlobal(conf);
		ensure(false);
	} catch (std::exception& e) {
		ensure(true);
	}
}

// Load a file with aliases referring to other aliases
template<> template<>
void to::test<8>()
{
	ifstream in("misc/rec-ts-alias.conf");
	ConfigFile conf;
	conf.parse(in, "misc/rec-ts-alias.conf");

	MatcherAliasDatabase::addGlobal(conf);
	Matcher m = Matcher::parse("timerange:f_3");
	//cerr << m << endl;
	ensure(true);
}

// Run matcher/*.txt files, doctest style
template<> template<>
void to::test<9>()
{
	runtime::readMatcherAliasDatabase();
	Lua L;

	// Define 'ensure' function
	string lua_ensure = "function ensure_matches(val, expr)\n"
			"  local matcher = arki.matcher.new(expr)\n"
			"  if (not matcher:match(val)) then error(expr .. ' did not match ' .. tostring(val) .. ':\\n' .. debug.traceback()) end\n"
			"end\n"
			"function ensure_not_matches(val, expr)\n"
			"  local matcher = arki.matcher.new(expr)\n"
			"  if (matcher:match(val)) then error(expr .. ' should not have matched ' .. tostring(val) .. ':\\n' .. debug.traceback()) end\n"
			"end\n"
			"function ensure_matchers_equal(expr1, expr2)\n"
			"  local matcher1 = arki.matcher.new(expr1)\n"
			"  local matcher2 = arki.matcher.new(expr2)\n"
			"  if matcher1:expanded() ~= matcher2:expanded() then error(tostring(matcher1) .. '(' .. matcher1:expanded() .. ') should be the same as ' .. tostring(matcher2) .. '(' .. matcher2:expanded() .. '):\\n' .. debug.traceback()) end\n"
			"end\n";
	if (luaL_dostring(L, lua_ensure.c_str()))
        {
                // Copy the error, so that it will exist after the pop
                string error = lua_tostring(L, -1);
                // Pop the error from the stack
                lua_pop(L, 1);
		ensure_equals(error, "");
        }
	
	// Run the various lua examples
	string path = "matcher";
	sys::fs::Directory dir(path);
	for (sys::fs::Directory::const_iterator d = dir.begin(); d != dir.end(); ++d)
	{
		if (!str::endsWith(*d, ".txt")) continue;
		string fname = str::joinpath(path, *d);
		if (luaL_dofile(L, fname.c_str()))
		{
			// Copy the error, so that it will exist after the pop
			string error = lua_tostring(L, -1);
			// Pop the error from the stack
			lua_pop(L, 1);
			ensure_equals(error, "");
		}
	}
}

template<> template<>
void to::test<10>()
{
	Matcher m1, m2;

	m1 = Matcher::parse("origin:GRIB1 OR BUFR\n    ");
	m2 = Matcher::parse("origin:GRIB1 OR BUFR;\n   \n;   \n  ;\n");
	ensure(m1.toString() == m2.toString());
}

}

// vim:set ts=4 sw=4:
