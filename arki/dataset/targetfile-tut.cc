/*
 * Copyright (C) 2007--2015  Enrico Zini <enrico@enricozini.org>
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

#include <arki/tests/tests.h>
#include <arki/dataset/targetfile.h>
#include <arki/types/reftime.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils/pcounter.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;

struct arki_dataset_targetfile_shar {
	ConfigFile config;
	Metadata md;

	arki_dataset_targetfile_shar()
	{
		// In-memory dataset configuration
		string conf =
			"[yearly]\n"
			"step = yearly\n"
			"\n"
			"[monthly]\n"
			"step = monthly\n"
			"\n"
			"[biweekly]\n"
			"step = biweekly\n"
			"\n"
			"[weekly]\n"
			"step = weekly\n"
			"\n"
			"[daily]\n"
			"step = daily\n"
			"\n"
			"[singlefile]\n"
			"step = singlefile\n"
			;

		stringstream incfg(conf);
		config.parse(incfg, "(memory)");

        md.set("reftime", "2007-06-05T04:03:02Z");
    }
};
TESTGRP(arki_dataset_targetfile);

namespace {
inline const matcher::Implementation& mimpl(const Matcher& m)
{
	return *m.m_impl->get(TYPE_REFTIME);
}
}

template<> template<>
void to::test<1>()
{
	unique_ptr<TargetFile> tf(TargetFile::create(*config.section("yearly")));

	ensure_equals((*tf)(md), "20/2007");
	ensure(tf->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:>2006"))));
	ensure(tf->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:<=2008"))));
	ensure(not tf->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:>2007"))));
	ensure(not tf->pathMatches("20/2007.test", mimpl(Matcher::parse("reftime:<2007"))));
}

template<> template<>
void to::test<2>()
{
	unique_ptr<TargetFile> tf(TargetFile::create(*config.section("monthly")));

	ensure_equals((*tf)(md), "2007/06");
}

template<> template<>
void to::test<3>()
{
	unique_ptr<TargetFile> tf(TargetFile::create(*config.section("biweekly")));

	ensure_equals((*tf)(md), "2007/06-1");
}

template<> template<>
void to::test<4>()
{
	unique_ptr<TargetFile> tf(TargetFile::create(*config.section("weekly")));

	ensure_equals((*tf)(md), "2007/06-1");
}

template<> template<>
void to::test<5>()
{
	unique_ptr<TargetFile> tf(TargetFile::create(*config.section("daily")));

	ensure_equals((*tf)(md), "2007/06-05");
}

template<> template<>
void to::test<6>()
{
	unique_ptr<TargetFile> tf(TargetFile::create(*config.section("singlefile")));

	ensure_equals((*tf)(md), "2007/06/05/04/1");
}


}

// vim:set ts=4 sw=4:
