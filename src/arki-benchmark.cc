/*
 * arki-benchmark - Run an arkimet import and query benchmark
 *
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <wibble/commandline/parser.h>
#include <wibble/string.h>
#include <wibble/sys/process.h>
#include <wibble/sys/fs.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif
#ifdef HAVE_DBALLE
#include <arki/scan/bufr.h>
#endif
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/utils.h>
#include <arki/runtime.h>
#include "bench/benchmark.h"

#include <fstream>
#include <iostream>
#include <sys/stat.h>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	BoolOption* list;

	Options() : StandardParserWithManpage("arki-benchmark", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] benchmarkdir [benchmark1 [benchmark2 ...]]";
		description =
			" read the dataset config file called 'benchmark.conf' inside the directory;"
		    " scan the directory for *.grib* and *.bufr files (case insensitive);"
			" import all the files found inside all the datasets found;"
			" scan the directory for *.query files and query all the datasets using"
			" the queries found in those files; output timing measurements of"
			" all these operations.";

		list = add<BoolOption>("list", 'l', "list", "",
				"list available benchmarks instead of running them");
	}
};

}
}

struct MetadataCounter : public metadata::Consumer
{
	size_t count;
	off_t size;

	MetadataCounter() : count(0), size(0) {}

    virtual bool operator()(Metadata& md)
    {
        ++count;
        size += md.source->getSize();
        return true;
    }
};

struct BenchmarkInfo
{
	ConfigFile cfg;
	std::string dirname;
	std::vector<std::string> gribs;
	std::vector<std::string> bufrs;
	std::vector< std::pair<std::string, Matcher> > queries;

};

struct ScanBenchmark : public Benchmark
{
	template<typename SCANNER>
	struct SubBenchmark : public Benchmark
	{
		const vector<string>& files;
		SCANNER scanner;

		SubBenchmark(const std::string& name, const vector<string>& files)
			: Benchmark(name), files(files), scanner(false) {}

		virtual void main()
		{
			using namespace wibble::sys;

			Metadata md;
			md.create();

			static const size_t iterations = 5;

			// Scan the files
			for (vector<string>::const_iterator i = files.begin();
					i != files.end(); ++i)
			{
				off_t size = fs::stat(*i)->st_size;
				int count;
				for (size_t j = 0; j < iterations; ++j)
				{
					count = 0;
					scanner.open(*i);
					while (scanner.next(md))
						++count;
					scanner.close();
				}
				double user, system, total;
				elapsed(user, system, total);
				double mps = count * iterations / total;
				double kbps = size * iterations / total / 1000;
				double avgsize = size / count;
				message("%s: %dm, %.0fmps, %.0fbpm, %.0fKbps",
						(*i).c_str(), count, mps, avgsize, kbps);
				timing("%d scan runs", iterations);
			}
		}
	};

	BenchmarkInfo& info;

	ScanBenchmark(BenchmarkInfo& info) : Benchmark("scan"), info(info)
	{
#ifdef HAVE_GRIBAPI
		if (!info.gribs.empty())
			addChild(new SubBenchmark<scan::Grib>("grib", info.gribs));
#endif
#ifdef HAVE_DBALLE
		if (!info.bufrs.empty())
			addChild(new SubBenchmark<scan::Bufr>("bufr", info.bufrs));
#endif
	}
};

struct DSBenchmark : public Benchmark
{
	BenchmarkInfo& info;
	ConfigFile& cfg;

	template<typename SCANNER>
	struct Importer : public Benchmark
	{
		DSBenchmark& b;
		WritableDataset* ds;
		const vector<string>& files;
		SCANNER scanner;

		Importer(DSBenchmark& b, const std::string& name, const vector<string>& files)
			: Benchmark(name), b(b), ds(0), files(files), scanner(true)
		{
			ds = WritableDataset::create(b.cfg);
		}
		~Importer()
		{
			if (ds) delete ds;
		}

		virtual void main()
		{
			using namespace wibble::sys;

			Metadata md;
			md.create();

			// Import the files
			size_t filecount = 0;
			size_t count = 0;
			size_t res_ok = 0, res_dup = 0, res_err = 0, res_unk = 0;
			off_t size = 0;
			for (vector<string>::const_iterator i = files.begin();
					i != files.end(); ++i)
			{
				++filecount;
				size += fs::stat(*i)->st_size;
				scanner.open(*i);
				while (scanner.next(md))
				{
					WritableDataset::AcquireResult res = ds->acquire(md);
					switch (res)
					{
						case WritableDataset::ACQ_OK: ++res_ok; break;
						case WritableDataset::ACQ_ERROR_DUPLICATE: ++res_dup; break;
						case WritableDataset::ACQ_ERROR: ++res_err; break;
						default: ++res_unk; break;
					}
					++count;
				}
				scanner.close();
			}
			double user, system, total;
			elapsed(user, system, total);
			double mps = count / total;
			double kbps = size / total / 1000;
			double avgsize = size / count;
			message("imported %d files: %dm, %.0fmps, %.0fbpm, %.0fKbps, %zd ok, %zd dup, %zd err, %zd unk",
					filecount, count, mps, avgsize, kbps,
					res_ok, res_dup, res_err, res_unk);
			timing("import");

			// Flush the dataset
			ds->flush();

			timing("flush");

			delete ds;
			ds = 0;
		}
	};

	struct Query : public Benchmark
	{
		DSBenchmark& b;
		ReadonlyDataset* ds;
		const Matcher& query;
		bool withData;

		Query(DSBenchmark& b, const std::string& name, const Matcher& query, bool withData = true)
			: Benchmark(name), b(b), ds(0), query(query), withData(withData)
		{
			ds = ReadonlyDataset::create(b.cfg);
		}
		~Query()
		{
			if (ds) delete ds;
		}

		virtual void main()
		{
			MetadataCounter mdc;
			dataset::DataQuery dq(query, withData);
			ds->queryData(dq, mdc);

			double user, system, total;
			elapsed(user, system, total);
			double mps = mdc.count / total;
			double kbps = mdc.size / total / 1000;
			double avgsize = mdc.count > 0 ? (mdc.size / mdc.count) : 0;
			timing("query: %dm, %.0fmps, %.0fbpm, %.0fKbps",
					mdc.count, mps, avgsize, kbps);

			Summary summary;
			ds->querySummary(query, summary);
			timing("querySummary: count %d", summary.count());

			delete ds;
			ds = 0;
		}
	};

	DSBenchmark(const std::string& name, ConfigFile& cfg, BenchmarkInfo& info)
		: Benchmark(name), info(info), cfg(cfg)
	{
		Benchmark* b = new Benchmark("import");
		addChild(b);
#ifdef HAVE_GRIBAPI
		if (!info.gribs.empty())
			b->addChild(new Importer<scan::Grib>(*this, "grib", info.gribs));
#endif
#ifdef HAVE_DBALLE
		if (!info.bufrs.empty())
			b->addChild(new Importer<scan::Bufr>(*this, "bufr", info.bufrs));
#endif
		b = new Benchmark("query");
		addChild(b);
		for (vector< pair<string, Matcher> >::const_iterator i = info.queries.begin();
				i != info.queries.end(); ++i)
			b->addChild(new Query(*this, i->first, i->second));
	}
	~DSBenchmark()
	{
	}

	virtual void main()
	{
	}
};

int main(int argc, const char* argv[])
{
	using namespace wibble::sys;
	using namespace wibble::str;

	wibble::commandline::Options opts;
	try {
		// We want predictable results
		srand(1);

		// We don't want buffered stdout
		if (setvbuf(stdout, NULL, _IOLBF, 0) != 0)
			throw wibble::exception::System("setting stdout to line-buffered mode");

		if (opts.parse(argc, argv))
			return 0;

		runtime::readMatcherAliasDatabase();

		// Benchmark data
		BenchmarkInfo info;

		// Read the benchmark directory name
		if (!opts.hasNext())
			throw wibble::exception::BadOption("you need to specify the directory name");
		info.dirname = opts.next();

		// Change into the benchmark directory
		process::chdir(info.dirname);

		// Scan the directory
		fs::Directory dir(info.dirname);
		for (fs::Directory::const_iterator i = dir.begin();
				i != dir.end(); ++i)
		{
			size_t pos = (*i).rfind('.');
			if (pos == string::npos)
				continue;
			string ext = (*i).substr(pos+1);
			if (ext.empty())
				continue;
			ext = tolower(ext);
			if (ext.substr(0, 4) == "grib")
				info.gribs.push_back(*i);
			else if (ext == "bufr")
				info.bufrs.push_back(*i);
			else if (ext == "query")
				info.queries.push_back(make_pair(*i, Matcher::parse(sys::fs::readFile(*i))));
			else
				continue;
		}

		// Create the scan benchmark
        Benchmark::root()->addChild(new ScanBenchmark(info));

		// Read the configuration
		runtime::parseConfigFile(info.cfg, joinpath(info.dirname, "benchmark.conf"));

		// Create the dataset benchmarks
		for (ConfigFile::section_iterator i = info.cfg.sectionBegin();
				i != info.cfg.sectionEnd(); ++i)
		{
			// Delete the dataset if it exists
			// FIXME: unsafe, and not needed if --list is used, but we're a
			// benchmark, not a general purpose tool
			system(("rm -rf " + i->second->value("path")).c_str());

			// (Re)create the dataset from scratch
			Benchmark::root()->addChild(new DSBenchmark(i->first, *i->second, info));
		}

		if (opts.list->boolValue())
		{
			Benchmark::root()->list(cout);
		}
		else if (opts.hasNext())
		{
			while (opts.hasNext())
			{
				string name = opts.next();
				if (name[0] == '/')
					name = name.substr(1);
				Benchmark::root()->run(name);
			}
		}
		else
			Benchmark::root()->run();

		return 0;
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}

// vim:set ts=4 sw=4:
