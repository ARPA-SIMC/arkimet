/// arki-check - Maintenance of arkimet dataset
#include "arki/libconfig.h"
#include "arki/exceptions.h"
#include "arki/utils/commandline/parser.h"
#include "arki/configfile.h"
#include "arki/datasets.h"
#include "arki/dataset/local.h"
#include "arki/dataset/segmented.h"
#include "arki/dataset/reporter.h"
#include "arki/metadata/consumer.h"
#include "arki/types/source/blob.h"
#include "arki/nag.h"
#include "arki/runtime.h"
#include "sstream"
#include "iostream"
#include "sys/stat.h"

//#define debug(...) fprintf(stderr, __VA_ARGS__)
#define debug(...) do{}while(0)

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace arki {
namespace runtime {

namespace {

struct Options : public BaseCommandLine
{
    commandline::VectorOption<commandline::String>* cfgfiles;
    commandline::BoolOption* verbose;
    commandline::BoolOption* debug;
    commandline::BoolOption* fix;
    commandline::BoolOption* accurate;
    commandline::BoolOption* repack;
    commandline::BoolOption* remove_all;
    commandline::BoolOption* tar;
    commandline::BoolOption* op_state;
    commandline::BoolOption* op_issue51;
    commandline::StringOption* op_remove;
    commandline::StringOption* op_unarchive;
    commandline::StringOption* restr;
    commandline::StringOption* filter;
    commandline::BoolOption* online;
    commandline::BoolOption* offline;

    Options() : BaseCommandLine("arki-check", 1)
    {
        using namespace commandline;

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
        tar = add<BoolOption>("tar", 0, "tar", "",
            "Convert directory segments into tar files");
        op_remove = add<StringOption>("remove", 0, "remove", "file",
            "Given metadata extracted from one or more datasets, remove it from the datasets where it is stored");
        op_unarchive = add<StringOption>("unarchive", 0, "unarchive", "file",
                "Given a pathname relative to .archive/last, move it out of the archive and back to the main dataset");
        op_state = add<BoolOption>("state", 0, "state", "",
                "Scan the dataset and print its state");
        op_issue51 = add<BoolOption>("issue51", 0, "issue51", "",
                "Check a dataset for corrupted terminator bytes (see issue #51)");
        restr = add<StringOption>("restrict", 0, "restrict", "names",
                "Restrict operations to only those datasets that allow one of the given (comma separated) names");
        filter = add<StringOption>("filter", 0, "filter", "matcher",
                "Restrict operations to at least those segments whose reference time matches the given matcher (but may be more)");
        online = add<BoolOption>("online", 0, "online", "",
                "Work on online data, skipping archives unless --offline is also provided");
        offline = add<BoolOption>("offline", 0, "offline", "",
                "Work on archives, skipping online data unless --online is also provided");
    }
};

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
    bool skip_archives = false;
    Matcher filter;

    ~Worker() {}
    virtual void process(const ConfigFile& cfg) = 0;
    virtual void done() = 0;
};

struct WorkerOnWritable : public Worker
{
    void process(const ConfigFile& cfg) override
    {
        unique_ptr<dataset::Checker> ds;
        try {
            auto config = dataset::Config::create(cfg);
            ds = config->create_checker();
            ds->skip_archives = skip_archives;
        } catch (std::exception& e) {
            throw SkipDataset(e.what());
        }
        operator()(*ds);
    }
    virtual void operator()(dataset::Checker& w) = 0;
};

struct Maintainer : public WorkerOnWritable
{
    bool fix;
    bool quick;

    Maintainer(bool fix, bool quick) : fix(fix), quick(quick)
    {
    }

    void operator()(dataset::Checker& w) override
    {
        dataset::OstreamReporter r(cerr);
        if (filter.empty())
            w.check(r, fix, quick);
        else
            w.check_filtered(filter, r, fix, quick);
    }

    void done() override {}
};

struct Repacker : public WorkerOnWritable
{
    bool fix;

    Repacker(bool fix) : fix(fix) {}

    void operator()(dataset::Checker& w) override
    {
        dataset::OstreamReporter r(cerr);
        if (filter.empty())
            w.repack(r, fix);
        else
            w.repack_filtered(filter, r, fix);
    }

    void done() override {}
};

struct RemoveAller : public WorkerOnWritable
{
    bool fix;

    RemoveAller(bool fix) : fix(fix) {}

    void operator()(dataset::Checker& w) override
    {
        dataset::OstreamReporter r(cout);
        if (filter.empty())
            w.remove_all(r, fix);
        else
            w.remove_all_filtered(filter, r, fix);
    }

    void done() {}
};

struct Tarrer : public WorkerOnWritable
{
    dataset::CheckerConfig& config;

    Tarrer(dataset::CheckerConfig& config) : config(config) {}

    void operator()(dataset::Checker& w) override
    {
        w.tar(config);
    }

    void done() {}
};

struct Unarchiver : public WorkerOnWritable
{
    std::string relpath;

    Unarchiver(const std::string& relpath) : relpath(relpath) {}

    void operator()(dataset::Checker& w) override
    {
        using namespace arki::dataset;
        if (segmented::Checker* c = dynamic_cast<segmented::Checker*>(&w))
            c->segment(relpath)->unarchive();
    }

    void done() override {}
};

struct Issue51 : public WorkerOnWritable
{
    bool fix;

    Issue51(bool fix) : fix(fix) {}

    void operator()(dataset::Checker& w) override
    {
        dataset::OstreamReporter r(cout);
        w.check_issue51(r, fix);
    }

    void done() override {}
};

struct PrintState : public WorkerOnWritable
{
    bool quick;

    PrintState(bool quick) : quick(quick) {}

    void operator()(dataset::Checker& w) override
    {
        using namespace arki::dataset;
        if (segmented::Checker* c = dynamic_cast<segmented::Checker*>(&w))
        {
            OstreamReporter r(cerr);
            segmented::State state;
            if (filter.empty())
                state = c->scan(r, quick);
            else
                state = c->scan_filtered(filter, r, quick);
            state.dump(stdout);
        }
    }

    void done() override {}
};

}


int arki_check(int argc, const char* argv[])
{
    Options opts;

    try {
        if (opts.parse(argc, argv))
            return 0;

        nag::init(opts.verbose->isSet(), opts.debug->isSet());

        runtime::init();

        set<string> dirs;

        unique_ptr<Worker> worker;

        size_t actionCount = 0;
        if (opts.repack->isSet()) ++actionCount;
        if (opts.remove_all->isSet()) ++actionCount;
        if (opts.tar->isSet()) ++actionCount;
        if (opts.op_remove->isSet()) ++actionCount;
        if (opts.op_unarchive->isSet()) ++actionCount;
        if (opts.op_state->isSet()) ++actionCount;
        if (opts.op_issue51->isSet()) ++actionCount;
        if (actionCount > 1)
            throw commandline::BadOption("only one of --repack, --remove, --remove-all, --tar, --unarchive, --state, or --issue51 can be given in one invocation");

        // Read the config file(s)
        runtime::Inputs inputs;
        for (const auto& pathname: opts.cfgfiles->values())
            inputs.add_config_file(pathname);
        while (opts.hasNext())
            inputs.add_config_file(opts.next());
        if (inputs.empty())
            throw commandline::BadOption("you need to specify the config file");

        // Remove unallowed entries
        if (opts.restr->isSet())
        {
            runtime::Restrict rest(opts.restr->stringValue());
            inputs.remove_unallowed(rest);
        }

        if (inputs.empty())
            throw std::runtime_error("No useable datasets found");

        if (opts.op_remove->isSet()) {
            if (opts.op_remove->stringValue().empty())
                throw commandline::BadOption("you need to give a file name to --remove");
            Datasets datasets(inputs.as_config());
            WriterPool pool(datasets);
            // Read all metadata from the file specified in --remove
            metadata::Collection todolist;
            todolist.read_from_file(opts.op_remove->stringValue());
            // Datasets where each metadata comes from
            vector<std::string> dsnames;
            // Verify that all metadata items can be mapped to a dataset
            unsigned count = 1;
            for (const auto& md: todolist)
            {
                if (!md->has_source_blob())
                {
                   stringstream ss;
                   ss << "cannot remove data #" << count << ": metadata does not come from an on-disk dataset";
                   throw std::runtime_error(ss.str());
                }

                auto ds = datasets.locate_metadata(*md);
                if (!ds)
                {
                   stringstream ss;
                   ss << "cannot remove data #" << count << " is does not come from any known dataset";
                   throw std::runtime_error(ss.str());
                }

                dsnames.push_back(ds->name);
                ++count;
            }
            // Perform removals
            count = 1;
            for (unsigned i = 0; i < todolist.size(); ++i)
            {
                auto ds = pool.get(dsnames[i]);
                try {
                    ds->remove(todolist[i]);
                } catch (std::exception& e) {
                    cerr << "Cannot remove message #" << count << ": " << e.what() << endl;
                }
                ++count;
            }
        } else {
            dataset::OstreamReporter reporter(cerr);
            dataset::CheckerConfig config(reporter, !opts.fix->boolValue());
            if (opts.filter->isSet())
                config.segment_filter = Matcher::parse(opts.filter->stringValue());

            if (opts.remove_all->boolValue())
                worker.reset(new RemoveAller(opts.fix->boolValue()));
            else if (opts.repack->boolValue())
                worker.reset(new Repacker(opts.fix->boolValue()));
            else if (opts.tar->boolValue())
            {
                if (opts.offline->isSet() && opts.online->isSet())
                {
                    config.offline = true;
                    config.online = true;
                } else if (opts.offline->isSet() && !opts.online->isSet()) {
                    config.offline = true;
                    config.online = false;
                } else if (!opts.offline->isSet() && opts.online->isSet()) {
                    config.offline = false;
                    config.online = true;
                } else {
                    config.offline = true;
                    config.online = false;
                }
                worker.reset(new Tarrer(config));
            }
            else if (opts.op_unarchive->boolValue())
                worker.reset(new Unarchiver(opts.op_unarchive->stringValue()));
            else if (opts.op_state->boolValue())
                worker.reset(new PrintState(not opts.accurate->boolValue()));
            else if (opts.op_issue51->boolValue())
                worker.reset(new Issue51(opts.fix->boolValue()));
            else
                worker.reset(new Maintainer(opts.fix->boolValue(),
                             not opts.accurate->boolValue()));

            if (opts.filter->isSet())
                worker->filter = Matcher::parse(opts.filter->stringValue());
            if (opts.online->isSet())
                worker->skip_archives = true;

            // Harvest the paths from it
            for (const ConfigFile& cfg: inputs)
            {
                try {
                    worker->process(cfg);
                } catch (SkipDataset& e) {
                    cerr << "Skipping dataset " << cfg.value("name") << ": " << e.what() << endl;
                    continue;
                }
            }

            worker->done();
        }
        return 0;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
    }
}

}
}
