/*
 * arki-check - Update dataset summaries
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>
#include <arki/configfile.h>
#include <arki/datasetpool.h>
#include <arki/dataset.h>
#include <arki/dataset/ondisk/writer.h>
#include <arki/dataset/ondisk/maintenance.h>
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/types/assigneddataset.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>
#include <sys/stat.h>

#include "config.h"

#if HAVE_DBALLE
#include <dballe++/init.h>
#endif

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
	BoolOption* fix;
	BoolOption* repack;
	BoolOption* invalidate;
	BoolOption* stats;
	StringOption* salvage;
	StringOption* op_remove;

	Options() : StandardParserWithManpage("arki-check", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] [configfile or directory...]";
		description =
		    "Given one or more dataset root directories (either specified directly or"
			" read from config files), perform a maintenance run on them."
			" Corrupted metadata files will be rebuilt, data files with deleted data"
			" will be packed, outdated summaries and indices will be regenerated.";
		cfgfiles = add< VectorOption<String> >("config", 'C', "config", "file",
			"read the configuration from the given file (can be given more than once)");
		fix = add<BoolOption>("fix", 'f', "fix", "",
			"Perform the changes instead of writing what would have been changed");
		repack = add<BoolOption>("repack", 'r', "repack", "",
			"Perform a repack instead of a check");
		invalidate = add<BoolOption>("invalidate", 0, "invalidate", "",
			"Create flagfiles to invalidate all metadata in a dataset.  Warning, this will work without requiring -f, and should always be invoked after a repack.");
		stats = add<BoolOption>("stats", 0, "stats", "",
			"Compute statistics about the various datasets.");
		salvage = add<StringOption>("salvage", 's', "salvage", "file",
			"Where to store metadata of duplicate items, if found in the dataset ('-' means standard output)");
		op_remove = add<StringOption>("remove", 0, "remove", "file",
			"Given metadata extracted from one or more datasets, remove it from the datasets where it is stored");
	}
};

}
}

struct MaintenanceReport : public dataset::ondisk::MaintenanceAgent
{
	size_t rebuild, fileSummary, fileReindex, dirSummary, index;
	bool needFullIndex;
	dataset::ondisk::Writer* writer;

	MaintenanceReport()
		: rebuild(0), fileSummary(0), fileReindex(0), dirSummary(0), index(0), needFullIndex(false), writer(0) {}
	virtual ~MaintenanceReport() {}

	void start(dataset::ondisk::Writer& w)
	{
		writer = &w;
	}
	void end()
	{
		writer = 0;
	}

	virtual void needsDatafileRebuild(dataset::ondisk::maint::Datafile& df)
	{
		++rebuild;
		cout << df.pathname << ": needs metadata rebuild." << endl;
	}
	virtual void needsSummaryRebuild(dataset::ondisk::maint::Datafile& df)
	{
		++fileSummary;
		cout << df.pathname << ": needs summary rebuild." << endl;
	}
	virtual void needsReindex(dataset::ondisk::maint::Datafile& df)
	{
		++fileReindex;
		cout << df.pathname << ": needs reindexing." << endl;
	}
	virtual void needsSummaryRebuild(dataset::ondisk::maint::Directory& df)
	{
		++dirSummary;
		cout << df.fullpath() << ": needs summary rebuild." << endl;
	}
	virtual void needsFullIndexRebuild(dataset::ondisk::maint::RootDirectory& df)
	{
		needFullIndex = true;
		cout << writer->path() << ": needs full index rebuild." << endl;
	}
	virtual void needsIndexRebuild(dataset::ondisk::maint::Datafile& df)
	{
		if (!needFullIndex)
		{
			++index;
			cout << df.pathname << ": needs index rebuild." << endl;
		}
	}

	std::string stfile(size_t count) const { return count > 1 ? "files" : "file"; }
	std::string stsumm(size_t count) const { return count > 1 ? "summaries" : "summary"; }

	void report()
	{
		bool done = false;
		cout << "Summary:";
		if (rebuild)
		{
			done = true;
			cout << endl << " " << rebuild << " metadata " << stfile(rebuild) << " to rebuild";
		}
		if (fileSummary)
		{
			done = true;
			cout << endl << " " << fileSummary << " data file " << stsumm(fileSummary) << " to rebuild";
		}
		if (fileReindex)
		{
			done = true;
			cout << endl << " " << fileReindex << " metadata " << stfile(fileReindex) << " to reindex";
		}
		if (dirSummary)
		{
			done = true;
			cout << endl << " " << dirSummary << " directory " << stsumm(dirSummary) << " to rebuild";
		}
		if (needFullIndex)
		{
			done = true;
			cout << endl << " the entire index needs to be rebuilt";
		}
		if (index)
		{
			done = true;
			cout << endl << " " << index << " index " << stfile(index) << " to reindex";
		}
		if (!done)
			cout << " nothing to do." << endl;
		else
			cout << "; rerun with -f to fix." << endl;
	}
};

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
		auto_ptr<struct stat> s = fs::stat(pathname);
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

struct Printer : public MetadataConsumer
{
	ostream& out;
	string outname;
	int processed;

	Printer(ostream& out, const string& outname)
		: out(out), outname(outname), processed(0) {}
	~Printer() {}

	virtual bool operator()(Metadata& md)
	{
		md.write(out, outname);
		++processed;
		return true;
	}
};

struct Worker
{
	~Worker() {}
	virtual void operator()(WritableDataset& w) = 0;
	virtual void done() = 0;
};

struct Maintainer : public Worker
{
	dataset::ondisk::MaintenanceAgent* ma;

	Maintainer(bool fix, MetadataConsumer* salvage = 0) : ma(0)
	{
		if (fix)
			if (salvage)
				ma = new dataset::ondisk::FullMaintenance(cerr, *salvage);
			else
				throw wibble::exception::Consistency("setting up maintenance run", "no metadata consumer given to salvage duplicate items");
		else
			ma = new MaintenanceReport;
	}
	~Maintainer()
	{
		if (ma) delete ma;
	}

	virtual void operator()(WritableDataset& w)
	{
		dataset::ondisk::Writer* ow = dynamic_cast<dataset::ondisk::Writer*>(&w);
		if (ow == 0)
			cerr << "Skipping dataset " << w.name() << ": not an ondisk dataset" << endl;
		else
			ow->maintenance(*ma);
	}

	virtual void done()
	{
		MaintenanceReport* mr = dynamic_cast<MaintenanceReport*>(ma);
		if (mr)
			mr->report();
	}
};

struct Repacker : public Worker
{
	bool fix;

	Repacker(bool fix) : fix(fix) {}

	virtual void operator()(WritableDataset& w)
	{
		w.repack(cout, fix);
	}

	virtual void done()
	{
	}
};

struct Invalidator : public Worker
{
	virtual void operator()(WritableDataset& w)
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

struct Statistician : public Worker
{
	Stats st;

	virtual void operator()(WritableDataset& w)
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


int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

#if HAVE_DBALLE
		dballe::DballeInit dballeInit;
#endif

		set<string> dirs;

		auto_ptr<Worker> worker;

		size_t actionCount = 0;
		if (opts.stats->boolValue()) ++actionCount;
		if (opts.invalidate->boolValue()) ++actionCount;
		if (opts.repack->boolValue()) ++actionCount;
		if (opts.op_remove->boolValue()) ++actionCount;
		if (actionCount > 1)
			throw wibble::exception::BadOption("only one of --stats, --invalidate, --repack and --remove can be given in one invocation");

		// Read the config file(s)
		ConfigFile cfg;
		bool foundConfig1 = runtime::parseConfigFiles(cfg, *opts.cfgfiles);
		bool foundConfig2 = runtime::parseConfigFiles(cfg, opts);
		if (!foundConfig1 && !foundConfig2)
			throw wibble::exception::BadOption("you need to specify the config file");

		if (opts.op_remove->isSet())
		{
			if (opts.op_remove->stringValue().empty())
				throw wibble::exception::BadOption("you need to give a file name to --remove");
			WritableDatasetPool pool(cfg);
			// Read all metadata from the file specified in --remove
			runtime::Input input(opts.op_remove->stringValue());
			Metadata md;
			vector< Item<types::AssignedDataset> > todolist;
			for (size_t count = 1; md.read(input.stream(), input.name()); ++count)
			{
				// Collect dataset name and ID of items to delete
				todolist.push_back(md.get(types::TYPE_ASSIGNEDDATASET).upcast<types::AssignedDataset>());
				if (!todolist.back().defined())
					throw wibble::exception::Consistency(
							"reading information on data to remove",
						   	"the metadata #" + str::fmt(count) + " is not assigned to any dataset");
			}
			// Perform removals
			size_t count = 1;
			for (vector< Item<types::AssignedDataset> >::const_iterator i = todolist.begin();
					i != todolist.end(); ++i, ++count)
			{
				WritableDataset* ds = pool.get((*i)->name);
				if (!ds)
				{
					cerr << "Message #" << count << " is not in any dataset: skipped" << endl;
					continue;
				}
				try {
					ds->remove((*i)->id);
				} catch (std::exception& e) {
					cerr << "Message #" << count << ": " << e.what() << endl;
				}
			}
		} else {
			auto_ptr<runtime::Output> salvageOutput;
			auto_ptr<Printer> salvagePrinter;

			if (opts.stats->boolValue())
				worker.reset(new Statistician());
			else if (opts.invalidate->boolValue())
				worker.reset(new Invalidator);
			else if (opts.repack->boolValue())
				worker.reset(new Repacker(opts.fix->boolValue()));
			else
			{
				if (opts.fix->boolValue() && !opts.salvage->isSet())
					throw wibble::exception::BadOption("please specify --salvage when using --fix");
				salvageOutput.reset(new runtime::Output(*opts.salvage));
				salvagePrinter.reset(new Printer(salvageOutput->stream(), salvageOutput->name()));
				worker.reset(new Maintainer(opts.fix->boolValue(), salvagePrinter.get()));
			}

			// Harvest the paths from it
			for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
					i != cfg.sectionEnd(); ++i)
			{
				auto_ptr<WritableDataset> ds;
				try {
					ds.reset(WritableDataset::create(*i->second));
				} catch (std::exception& e) {
					cerr << "Skipping dataset " << i->first << ": " << e.what() << endl;
					continue;
				}

				(*worker)(*ds.get());
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
