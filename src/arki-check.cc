/*
 * arki-check - Update dataset summaries
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/wibble/exception.h>
#include <arki/wibble/commandline/parser.h>
#include <arki/wibble/sys/fs.h>
#include <arki/wibble/string.h>
#include <arki/configfile.h>
#include <arki/metadata/printer.h>
#include <arki/datasetpool.h>
#include <arki/dataset/local.h>
#include <arki/metadata/consumer.h>
#include <arki/types/assigneddataset.h>
#include <arki/nag.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>
#include <sys/stat.h>

//#define debug(...) fprintf(stderr, __VA_ARGS__)
#define debug(...) do{}while(0)

using namespace std;
using namespace arki;
using namespace wibble;
using namespace wibble::sys;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	VectorOption<String>* cfgfiles;
	BoolOption* verbose;
	BoolOption* debug;
	BoolOption* fix;
	BoolOption* accurate;
	BoolOption* repack;
	BoolOption* invalidate;
	BoolOption* remove_all;
	BoolOption* stats;
	OptvalIntOption* scantest;
	StringOption* op_remove;
	StringOption* restr;

	Options() : StandardParserWithManpage("arki-check", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] [configfile or directory...]";
		description =
			"Given one or more dataset root directories (either specified directly or"
			" read from config files), perform a maintenance run on them."
			" Corrupted metadata files will be rebuilt, data files with deleted data"
			" will be packed, outdated summaries and indices will be regenerated.";
		debug = add<BoolOption>("debug", 0, "debug", "", "debug output");
		verbose = add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
		cfgfiles = add< VectorOption<String> >("config", 'C', "config", "file",
			"read the configuration from the given file (can be given more than once)");
		fix = add<BoolOption>("fix", 'f', "fix", "",
			"Perform the changes instead of writing what would have been changed");
		accurate = add<BoolOption>("accurate", 0, "accurate", "",
			"Also verify the consistency of the contents of the data files (slow)");
		repack = add<BoolOption>("repack", 'r', "repack", "",
			"Perform a repack instead of a check");
		remove_all = add<BoolOption>("remove-all", 0, "remove-all", "",
			"Completely empty the dataset, removing all data and metadata");
		invalidate = add<BoolOption>("invalidate", 0, "invalidate", "",
			"Create flagfiles to invalidate all metadata in a dataset.	Warning, this will work without requiring -f, and should be invoked only after a repack.");
		stats = add<BoolOption>("stats", 0, "stats", "",
			"Compute statistics about the various datasets.");
		op_remove = add<StringOption>("remove", 0, "remove", "file",
			"Given metadata extracted from one or more datasets, remove it from the datasets where it is stored");
		restr = add<StringOption>("restrict", 0, "restrict", "names",
			"restrict operations to only those datasets that allow one of the given (comma separated) names");
        scantest = add<OptvalIntOption>("scantest", 0, "scantest", "idx",
            "Output metadata for data in the datasets that cannot be scanned or does not match the dataset filter."
            " Sample the data at position idx (starting from 0) in each file in the dataset."
            " If idx is omitted, it defaults to 0 (the first one)");
    }

    /**
     * Parse the config files from the datasets found in the remaining
     * commandline arguments
     *
     * Return true if at least one config file was found in \a opts
     */
    bool readDatasetConfig(ConfigFile& cfg)
    {
        bool found = false;
        while (hasNext())
        {
            ReadonlyDataset::readConfig(next(), cfg);
            found = true;
        }
        return found;
    }
};

}
}

#if 0
struct Stats : public dataset::ondisk::Visitor
{
	string name;

	// Total
	size_t totData;
	size_t totMd;
	size_t totSum;
	size_t totIdx;

	// Current dataset
	size_t curData;
	size_t curMd;
	size_t curSum;
	size_t curIdx;

	Stats() :
		totData(0), totMd(0), totSum(0), totIdx(0) {}

	size_t fsize(const std::string& pathname)
	{
		unique_ptr<struct stat> s = fs::stat(pathname);
		if (s.get() == 0)
			return 0;
		else
			return s->st_size;
	}

	virtual void enterDataset(dataset::ondisk::Writer& w)
	{
		name = w.path();
		curData = curMd = curSum = curIdx = 0;
		curIdx = fsize(str::joinpath(w.path(), "index.sqlite"));
	}
	virtual void leaveDataset(dataset::ondisk::Writer&)
	{
		printf("%s: %.1fMb data, %.1fMb metadata, %.1fMb summaries, %.1fMb index\n",
				name.c_str(), curData/1000000.0, curMd / 1000000.0, curSum / 1000000.0, curIdx / 1000000.0);

		double total = curData + curMd + curSum + curIdx;
		printf("%s: %.1fMb total: %.1f%% data, %.1f%% metadata, %.1f%% summaries, %.1f%% index\n",
				name.c_str(), total, curData*100/total, curMd * 100 / total, curSum * 100 / total, curIdx * 100 / total);

		totData += curData;
		totMd += curMd;
		totSum += curSum;
		totIdx += curIdx;
	}

	virtual void enterDirectory(dataset::ondisk::maint::Directory& dir)
	{
		curSum += fsize(str::joinpath(dir.fullpath(), "summary"));
	}
	virtual void leaveDirectory(dataset::ondisk::maint::Directory& dir)
	{
	}

	virtual void visitFile(dataset::ondisk::maint::Datafile& file)
	{
		curData += fsize(file.pathname);
		curMd += fsize(file.pathname + ".metadata");
		curSum += fsize(file.pathname + ".summary");
	}

	void stats()
	{
		printf("total: %.1fMb data, %.1fMb metadata, %.1fMb summaries, %.1fMb index\n",
				totData/1000000.0, totMd / 1000000.0, totSum / 1000000.0, totIdx / 1000000.0);

		double total = totData + totMd + totSum + totIdx;
		printf("total: %.1fMb total: %.1f%% data, %.1f%% metadata, %.1f%% summaries, %.1f%% index\n",
				total/1000000.0, totData*100/total, totMd * 100 / total, totSum * 100 / total, totIdx * 100 / total);
	}
};
#endif

struct SkipDataset : public std::exception
{
    string msg;

    SkipDataset(const std::string& msg)
        : msg(msg) {}
    virtual ~SkipDataset() throw () {}

    virtual const char* what() const throw() { return msg.c_str(); }
};

struct Worker
{
	~Worker() {}
	virtual void process(const ConfigFile& cfg) = 0;
	virtual void done() = 0;
};

struct WorkerOnWritable : public Worker
{
    virtual void process(const ConfigFile& cfg)
    {
        unique_ptr<dataset::WritableLocal> ds;
        try {
            ds.reset(dataset::WritableLocal::create(cfg));
        } catch (std::exception& e) {
            throw SkipDataset(e.what());
        }
        operator()(*ds);
    }
    virtual void operator()(dataset::WritableLocal& w) = 0;
};

struct Maintainer : public WorkerOnWritable
{
	bool fix;
	bool quick;

	Maintainer(bool fix, bool quick) : fix(fix), quick(quick)
	{
	}

	virtual void operator()(dataset::WritableLocal& w)
	{
		w.check(cerr, fix, quick);
	}

	virtual void done()
	{
	}
};

struct Repacker : public WorkerOnWritable
{
	bool fix;

	Repacker(bool fix) : fix(fix) {}

	virtual void operator()(dataset::WritableLocal& w)
	{
		w.repack(cout, fix);
		w.flush();
	}

	virtual void done()
	{
	}
};

struct RemoveAller : public WorkerOnWritable
{
	bool fix;

	RemoveAller(bool fix) : fix(fix) {}

	virtual void operator()(dataset::WritableLocal& w)
	{
		w.removeAll(cout, fix);
		w.flush();
	}

	virtual void done()
	{
	}
};

struct Counter : public metadata::Eater
{
    metadata::Eater& next;
    size_t count;

    Counter(metadata::Eater& next) : next(next), count(0) {}

    bool eat(unique_ptr<Metadata>&& md) override
    {
        ++count;
        return next.eat(move(md));
    }
};

struct ScanTest : public Worker
{
    runtime::Output out; // Default output to stdout
    metadata::BinaryPrinter printer;
    size_t idx;

    ScanTest(size_t idx=0) : printer(out), idx(idx) {}

    virtual void process(const ConfigFile& cfg)
    {
        unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(cfg));
        Counter counter(printer);
        if (dataset::Local* ld = dynamic_cast<dataset::Local*>(ds.get()))
        {
            counter.count = 0;
            size_t total = ld->scan_test(counter, idx);
            if (counter.count)
                nag::warning("%s: %zd/%zd samples with problems at index %zd",
                        cfg.value("name").c_str(), counter.count, total, idx);
            else if (total)
                nag::verbose("%s: %zd samples ok at index %zd",
                        cfg.value("name").c_str(), total, idx);
            else
                nag::verbose("%s: no samples found at index %zd",
                        cfg.value("name").c_str(), idx);
        } else {
            throw SkipDataset("dataset is not a local dataset");
        }
    }

    virtual void done()
    {
    }
};


#if 0
struct Invalidator : public Worker
{
	virtual void operator()(dataset::WritableLocal& w)
	{
		dataset::ondisk::Writer* ow = dynamic_cast<dataset::ondisk::Writer*>(&w);
		if (ow == 0)
			cerr << "Skipping dataset " << w.name() << ": not an ondisk dataset" << endl;
		else
			ow->invalidateAll();
	}

	virtual void done()
	{
	}
};
#endif

#if 0
struct Statistician : public Worker
{
	Stats st;

	virtual void operator()(dataset::WritableLocal& w)
	{
		dataset::ondisk::Writer* ow = dynamic_cast<dataset::ondisk::Writer*>(&w);
		if (ow == 0)
			cerr << "Skipping dataset " << w.name() << ": not an ondisk dataset" << endl;
		else
			ow->depthFirstVisit(st);
	}

	virtual void done()
	{
		st.stats();
	}
};
#endif


int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		nag::init(opts.verbose->isSet(), opts.debug->isSet());

		runtime::init();

		set<string> dirs;

		unique_ptr<Worker> worker;

        size_t actionCount = 0;
        if (opts.stats->isSet()) ++actionCount;
        if (opts.invalidate->isSet()) ++actionCount;
        if (opts.repack->isSet()) ++actionCount;
        if (opts.remove_all->isSet()) ++actionCount;
        if (opts.op_remove->isSet()) ++actionCount;
        if (opts.scantest->isSet()) ++actionCount;
        if (actionCount > 1)
            throw wibble::exception::BadOption("only one of --stats, --invalidate, --repack, --remove, --remove-all or --scantest can be given in one invocation");

		// Read the config file(s)
		ConfigFile cfg;
		bool foundConfig1 = runtime::parseConfigFiles(cfg, *opts.cfgfiles);
		bool foundConfig2 = opts.readDatasetConfig(cfg);
		if (!foundConfig1 && !foundConfig2)
			throw wibble::exception::BadOption("you need to specify the config file");

		// Remove unallowed entries
		if (opts.restr->isSet())
		{
			runtime::Restrict rest(opts.restr->stringValue());
			rest.remove_unallowed(cfg);
		}

		if (opts.op_remove->isSet())
		{
			if (opts.op_remove->stringValue().empty())
				throw wibble::exception::BadOption("you need to give a file name to --remove");
			WritableDatasetPool pool(cfg);
			// Read all metadata from the file specified in --remove
			runtime::Input input(opts.op_remove->stringValue());
			// Collect metadata to delete
			metadata::Collection todolist;
			for (size_t count = 1; ; ++count)
			{
                unique_ptr<Metadata> md(new Metadata);
                if (!md->read(input.stream(), input.name())) break;
                const types::AssignedDataset* ad = md->get<types::AssignedDataset>();
                if (!ad) throw wibble::exception::Consistency(
                        "reading information on data to remove",
                        "the metadata #" + str::fmt(count) + " is not assigned to any dataset");
                todolist.eat(move(md));
			}
			// Perform removals
			size_t count = 1;
			for (metadata::Collection::const_iterator i = todolist.begin();
					i != todolist.end(); ++i, ++count)
			{
                const types::AssignedDataset* ad = (*i)->get<types::AssignedDataset>();
                WritableDataset* ds = pool.get(ad->name);
				if (!ds)
				{
					cerr << "Message #" << count << " is not in any dataset: skipped" << endl;
					continue;
				}
				try {
                    ds->remove(**i);
				} catch (std::exception& e) {
					cerr << "Message #" << count << ": " << e.what() << endl;
				}
			}
		} else {
			if (opts.stats->boolValue())
				// worker.reset(new Statistician());
				throw wibble::exception::Consistency("running --stats", "statistics code needs to be reimplemented");
			else if (opts.invalidate->boolValue())
				// worker.reset(new Invalidator);
				throw wibble::exception::Consistency("running --invalidate", "invalidate code needs to be reimplemented");
			else if (opts.remove_all->boolValue())
				worker.reset(new RemoveAller(opts.fix->boolValue()));
			else if (opts.repack->boolValue())
				worker.reset(new Repacker(opts.fix->boolValue()));
            else if (opts.scantest->isSet())
            {
                size_t idx = 0;
                if (opts.scantest->hasValue())
                    idx = opts.scantest->value();
                worker.reset(new ScanTest(idx));
            }
			else
				worker.reset(new Maintainer(opts.fix->boolValue(),
						not opts.accurate->boolValue()));

            // Harvest the paths from it
            for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
                    i != cfg.sectionEnd(); ++i)
            {
                try {
                    worker->process(*i->second);
                } catch (SkipDataset& e) {
                    cerr << "Skipping dataset " << i->first << ": " << e.what() << endl;
                    continue;
                }
            }

            worker->done();
		}
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}

	return 0;
}

// vim:set ts=4 sw=4:
