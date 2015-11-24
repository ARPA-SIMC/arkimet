/*
 * Copyright (C) 2008--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/tests.h>
#include <arki/metadata/collection.h>
#include <arki/sort.h>
#include <arki/types/reftime.h>
#include <arki/types/run.h>
#include <arki/utils/string.h>

#include <sstream>
#include <iostream>

namespace std{
static ostream& operator<<(ostream& out, const vector<int>& v)
{
	return out << wibble::str::join(v.begin(), v.end());
}
}

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_sort_shar {
    arki_sort_shar()
    {
    }


};
TESTGRP(arki_sort);

namespace {
void produce(int hour, int minute, int run, metadata::Eater& c)
{
    unique_ptr<Metadata> md(new Metadata);
    md->set(Reftime::createPosition(Time(2008, 7, 6, hour, minute, 0)));
    md->set(Run::createMinute(run));
    c.eat(move(md));
}

vector<int> mdvals(const Metadata& md)
{
    const reftime::Position* rt = dynamic_cast<const reftime::Position*>(md.get<Reftime>());
    const run::Minute* run = dynamic_cast<const run::Minute*>(md.get<Run>());
    vector<int> res;
    res.push_back(rt->time.vals[3]);
    res.push_back(rt->time.vals[4]);
    res.push_back(run->minute()/60);
    return res;
}

vector<int> mdvals(int h, int m, int r)
{
	vector<int> res;
	res.push_back(h);
	res.push_back(m);
	res.push_back(r);
	return res;
}
}

// Test a simple case
template<> template<>
void to::test<1>()
{
	metadata::Collection mdc;
	refcounted::Pointer<sort::Compare> cmp = sort::Compare::parse("hour:run,-reftime");
	sort::Stream sorter(*cmp, mdc);

	produce(0, 0, 10, sorter);
	produce(0, 1, 9, sorter);
	produce(0, 3, 7, sorter);
	produce(0, 2, 9, sorter);
	produce(1, 0, 8, sorter);
	produce(1, 1, 9, sorter);
	produce(1, 2, 7, sorter);

	sorter.flush();

	ensure_equals(mdvals(mdc[0]), mdvals(0, 3, 7));
	ensure_equals(mdvals(mdc[1]), mdvals(0, 2, 9));
	ensure_equals(mdvals(mdc[2]), mdvals(0, 1, 9));
	ensure_equals(mdvals(mdc[3]), mdvals(0, 0, 10));
	ensure_equals(mdvals(mdc[4]), mdvals(1, 2, 7));
	ensure_equals(mdvals(mdc[5]), mdvals(1, 0, 8));
	ensure_equals(mdvals(mdc[6]), mdvals(1, 1, 9));
}

// An empty expression sorts by reference time
template<> template<>
void to::test<2>()
{
	metadata::Collection mdc;
	refcounted::Pointer<sort::Compare> cmp = sort::Compare::parse("");
	sort::Stream sorter(*cmp, mdc);

	produce(1, 0, 8, sorter);
	produce(1, 1, 9, sorter);
	produce(1, 2, 7, sorter);
	produce(0, 0, 10, sorter);
	produce(0, 1, 9, sorter);
	produce(0, 3, 7, sorter);
	produce(0, 2, 9, sorter);

	sorter.flush();

	ensure_equals(mdvals(mdc[0]), mdvals(0, 0, 10));
	ensure_equals(mdvals(mdc[1]), mdvals(0, 1, 9));
	ensure_equals(mdvals(mdc[2]), mdvals(0, 2, 9));
	ensure_equals(mdvals(mdc[3]), mdvals(0, 3, 7));
	ensure_equals(mdvals(mdc[4]), mdvals(1, 0, 8));
	ensure_equals(mdvals(mdc[5]), mdvals(1, 1, 9));
	ensure_equals(mdvals(mdc[6]), mdvals(1, 2, 7));
}

}

// vim:set ts=4 sw=4:
