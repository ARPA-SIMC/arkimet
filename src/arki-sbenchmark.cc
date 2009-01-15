/*
 * arki-sbenchmark - Benchmark summary performance
 *
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"
#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/reftime.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/summary-old.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/runtime.h>
#include "bench/benchmark.h"

#include "config.h"

#include <vector>
#include <iostream>
#include <time.h>
#include <stdlib.h>

using namespace std;
using namespace arki;

time_t seconds(const types::Time& t)
{
	struct tm stm;
	stm.tm_year = t.vals[0] - 1900;
	stm.tm_mon = t.vals[1] - 1;
	stm.tm_mday = t.vals[2];
	stm.tm_hour = t.vals[3];
	stm.tm_min = t.vals[4];
	stm.tm_sec = t.vals[5];
	return mktime(&stm);
}

Item<types::Time> fromSeconds(const time_t& t)
{
	struct tm* stm = gmtime(&t);
	return types::Time::create(stm->tm_year + 1900, stm->tm_mon + 1, stm->tm_mday, stm->tm_hour, stm->tm_min, stm->tm_sec);
}

template<typename N>
N randrange(const N& min, const N& max)
{
	return min + (random() * (max - min) / RAND_MAX);
}

struct MDFiller
{
	vector<Metadata> mds;
	vector<size_t> order;

	void addRandomFields(Metadata& md, time_t ts, time_t te, const oldsummary::Stats& st)
	{
		// Reftime
		md.set(types::reftime::Position::create(fromSeconds(randrange(ts, te))));
		// Source
		md.source = types::source::Inline::create("GRIB", randrange(st.size * 0.7, st.size * 1.3));

		// TYPE_NOTE = 6,
		// TYPE_ASSIGNEDDATASET = 8,
	}

	MDFiller()
	{
		// Read sample metadata combinations from a large summary
		string sample = "arki-sbenchmark.summary";
		ifstream in(sample.c_str());
		OldSummary data;
		data.read(in, sample);

		for (OldSummary::const_iterator i = data.begin();
				i != data.end(); ++i)
		{
			time_t ts = seconds(*i->second->reftimeMerger.begin);
			time_t te = i->second->reftimeMerger.end.defined() ? seconds(*i->second->reftimeMerger.end) : ts;
			for (size_t c = 0; c < i->second->count; ++c)
			{
				// Base metadata with the data in the item
				Metadata md;
				for (oldsummary::Item::const_iterator j = i->first->begin();
						j != i->first->end(); ++j)
					md.set(j->second);
				addRandomFields(md, ts, te, *i->second);
				mds.push_back(md);
				order.push_back(order.size());
			}
		}

		// Permutation vector
		for (size_t i = 0; i < order.size()/2; ++i)
		{
			size_t a = randrange((size_t)0, order.size());
			size_t b = randrange((size_t)0, order.size());
			size_t t = order[a];
			order[a] = order[b];
			order[b] = t;
		}
	}
};

struct BenchData
{
	MDFiller mdset;
}* bdata;

template<typename SUMMARY>
struct SummaryBenchmark : public Benchmark
{
	struct Create : public Benchmark
	{
		Create() : Benchmark("create") {}

		virtual void main()
		{
			static const size_t create_iterations = 3;
			static const size_t encode_iterations = 20;
			static const size_t decode_iterations = 20;
			static const size_t matcher_iterations = 10000;

			Matcher matcher = Matcher::parse("reftime:=2008-05-15; product:GRIB1,200,2,11");

			SUMMARY s;
			for (size_t it = 0; it < create_iterations; ++it)
			{
				s.clear();
				for (vector<size_t>::const_iterator i = bdata->mdset.order.begin();
						i != bdata->mdset.order.end(); ++i)
					s.add(bdata->mdset.mds[*i]);
			}
			timing("%d create runs", create_iterations);

			string encoded;
			for (size_t it = 0; it < encode_iterations; ++it)
				encoded = s.encode();
			timing("%d encode runs, %zdb", encode_iterations, encoded.size());

			size_t count = 0;
			for (size_t it = 0; it < decode_iterations; ++it)
			{
				std::stringstream reader(encoded);
				SUMMARY dec;
				dec.read(reader, "(memory)");
				count += dec.count();
			}
			timing("%d decode runs (%d aggregate count)", decode_iterations, count);

			bool matcherResult;
			for (size_t it = 0; it < matcher_iterations; ++it)
				matcherResult = matcher(s);
			timing("%d match runs (result: %d)", matcher_iterations, (int)matcherResult);

			#if 0
			using namespace wibble::sys;
				double user, system, total;
				elapsed(user, system, total);
				double mps = count * iterations / total;
				double kbps = size * iterations / total / 1000;
				double avgsize = size / count;
				message("%s: %dm, %.0fmps, %.0fbpm, %.0fKbps",
						(*i).c_str(), count, mps, avgsize, kbps);
				timing("%d scan runs", iterations);
			#endif
		}
	};

	SummaryBenchmark(const std::string& name) : Benchmark(name)
	{
		addChild(new Create());
	}
};

int main(int argc, const char* argv[])
{
	using namespace wibble::sys;
	using namespace wibble::str;

	try {
		// We want predictable results
		srand(1);

		// We don't want buffered stdout
		if (setvbuf(stdout, NULL, _IOLBF, 0) != 0)
			throw wibble::exception::System("setting stdout to line-buffered mode");

		runtime::readMatcherAliasDatabase();

		// Benchmark data
		cerr << "Loading test data... ";
		bdata = new BenchData;
		cerr << "done." << endl;

		// Populate benchmarks
        Benchmark::root()->addChild(new SummaryBenchmark<OldSummary>("old"));
        Benchmark::root()->addChild(new SummaryBenchmark<Summary>("inuse"));
        //Benchmark::root()->addChild(new SummaryBenchmark<NewSummary>("new"));

		Benchmark::root()->run();

		return 0;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}

// vim:set ts=4 sw=4:
