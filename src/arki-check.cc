/// arki-check - Maintenance of arkimet dataset

#include "config.h"
#include <arki/wibble/exception.h>
#include <arki/utils/commandline/parser.h>
#include <arki/configfile.h>
#include <arki/runtime/printer.h>
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
using namespace arki::utils;

namespace arki {
namespace utils {
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
            dataset::Reader::readConfig(next(), cfg);
            found = true;
        }
        return found;
    }
};

}
}
}

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
    void process(const ConfigFile& cfg) override
    {
        unique_ptr<dataset::LocalChecker> ds;
        try {
            ds.reset(dataset::LocalChecker::create(cfg));
        } catch (std::exception& e) {
            throw SkipDataset(e.what());
        }
        operator()(*ds);
    }
    virtual void operator()(dataset::LocalChecker& w) = 0;
};

struct Maintainer : public WorkerOnWritable
{
	bool fix;
	bool quick;

	Maintainer(bool fix, bool quick) : fix(fix), quick(quick)
	{
	}

    void operator()(dataset::LocalChecker& w) override
    {
        dataset::OstreamReporter r(cerr);
        w.check(r, fix, quick);
    }

    void done() override {}
};

struct Repacker : public WorkerOnWritable
{
	bool fix;

	Repacker(bool fix) : fix(fix) {}

    void operator()(dataset::LocalChecker& w) override
    {
        dataset::OstreamReporter r(cout);
        w.repack(r, fix);
    }

    void done() override {}
};

struct RemoveAller : public WorkerOnWritable
{
	bool fix;

	RemoveAller(bool fix) : fix(fix) {}

    void operator()(dataset::LocalChecker& w) override
    {
        dataset::OstreamReporter r(cout);
        w.removeAll(r, fix);
    }

    void done() {}
};

struct ScanTest : public Worker
{
    runtime::Stdout out; // Default output to stdout
    metadata::BinaryPrinter printer;
    size_t idx;

    ScanTest(size_t idx=0) : printer(out), idx(idx) {}

    void process(const ConfigFile& cfg) override
    {
        unique_ptr<dataset::Reader> ds(dataset::Reader::create(cfg));
        if (dataset::LocalReader* ld = dynamic_cast<dataset::LocalReader*>(ds.get()))
        {
            size_t count = 0;
            size_t total = ld->scan_test([&](unique_ptr<Metadata> md) {
                ++count;
                return printer.eat(move(md));
            }, idx);
            if (count)
                nag::warning("%s: %zd/%zd samples with problems at index %zd",
                        cfg.value("name").c_str(), count, total, idx);
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

    void done() override
    {
    }
};


#if 0
struct Invalidator : public Worker
{
	virtual void operator()(dataset::LocalWriter& w)
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

	virtual void operator()(dataset::LocalWriter& w)
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
    commandline::Options opts;
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
            throw commandline::BadOption("only one of --stats, --invalidate, --repack, --remove, --remove-all or --scantest can be given in one invocation");

        // Read the config file(s)
        ConfigFile cfg;
        bool foundConfig1 = runtime::parseConfigFiles(cfg, *opts.cfgfiles);
        bool foundConfig2 = opts.readDatasetConfig(cfg);
        if (!foundConfig1 && !foundConfig2)
            throw commandline::BadOption("you need to specify the config file");

		// Remove unallowed entries
		if (opts.restr->isSet())
		{
			runtime::Restrict rest(opts.restr->stringValue());
			rest.remove_unallowed(cfg);
		}

        if (opts.op_remove->isSet())
        {
            if (opts.op_remove->stringValue().empty())
                throw commandline::BadOption("you need to give a file name to --remove");
            WriterPool pool(cfg);
            // Read all metadata from the file specified in --remove
            metadata::Collection todolist(opts.op_remove->stringValue());
            // Verify that all metadata items have AssignedDataset set
            unsigned count = 1;
            for (const auto& md: todolist)
            {
                const types::AssignedDataset* ad = md->get<types::AssignedDataset>();
                if (!ad)
                {
                   stringstream ss;
                   ss << "cannot read information on data to remove: the metadata #" << count << " is not assigned to any dataset";
                   throw std::runtime_error(ss.str());
                }
                ++count;
            }
            // Perform removals
            count = 1;
            for (const auto& md: todolist)
            {
                const types::AssignedDataset* ad = md->get<types::AssignedDataset>();
                dataset::Writer* ds = pool.get(ad->name);
                if (!ds)
                {
                    cerr << "Message #" << count << " is not in any dataset: skipped" << endl;
                    continue;
                }
                try {
                    ds->remove(*md);
                } catch (std::exception& e) {
                    cerr << "Message #" << count << ": " << e.what() << endl;
                }
                ++count;
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
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}

	return 0;
}
