/*
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher/test-utils.h>
#include <arki/report.h>
#include <arki/summary.h>
#include <arki/types.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_report_shar {
    Metadata md;

    arki_report_shar()
    {
        md.create();
        arki::tests::fill(md);
	}
};
TESTGRP(arki_report);

// Test a simple case
template<> template<>
void to::test<1>()
{
	Report rep;
	std::stringstream res;
	rep.captureOutput(res);
	rep.loadString(
		"Report = { \n"
		"  mdcount = 0, \n"
		"  sumcount = 0, \n"
		"  readMetadata = function(self, md) \n"
		"    self.mdcount = self.mdcount + 1 \n"
		"  end, \n"
		"  readSummary = function(self, s) \n"
		"    self.sumcount = self.sumcount + 1 \n"
		"  end, \n"
		"  report = function(self) \n"
		"    print(self.mdcount, self.sumcount) \n"
		"  end \n"
		"} \n"
	);
	
	rep.report();
	ensure_equals(res.str(), "0\t0\n");

	res.str(string());
	rep(md);
	rep.report();
	ensure_equals(res.str(), "1\t0\n");

	Summary s;
	res.str(string());
	rep(s);
	rep.report();
	ensure_equals(res.str(), "1\t1\n");
}

}

// vim:set ts=4 sw=4:
