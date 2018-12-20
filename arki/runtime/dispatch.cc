#include "dispatch.h"
#include "processor.h"
#include "module.h"
#include "arki/runtime.h"
#include "arki/runtime/config.h"
#include "arki/dispatcher.h"
#include "arki/utils.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include "arki/validator.h"
#include "arki/types/source/blob.h"
#include <sys/time.h>
#include <iostream>

using namespace std;
using namespace arki::utils;

#if __xlC__
// From glibc
#define timersub(a, b, result)                                                \
  do {                                                                        \
    (result)->tv_sec = (a)->tv_sec - (b)->tv_sec;                             \
    (result)->tv_usec = (a)->tv_usec - (b)->tv_usec;                          \
    if ((result)->tv_usec < 0) {                                              \
      --(result)->tv_sec;                                                     \
      (result)->tv_usec += 1000000;                                           \
    }                                                                         \
  } while (0)
#endif

namespace arki {
namespace runtime {

DispatchOptions::DispatchOptions(ScanCommandLine& args)
{
    using namespace arki::utils::commandline;

    dispatchOpts = args.addGroup("Options controlling dispatching data to datasets");

    moveok = dispatchOpts->add<StringOption>("moveok", 0, "moveok", "directory",
            "move input files imported successfully to the given directory "
            "(destination directory must be on the same filesystem of source file)");
    moveko = dispatchOpts->add<StringOption>("moveko", 0, "moveko", "directory",
            "move input files with problems to the given directory "
            "(destination directory must be on the same filesystem of source file)");
    movework = dispatchOpts->add<StringOption>("movework", 0, "movework", "directory",
            "move input files here before opening them. This is useful to "
            "catch the cases where arki-scan crashes without having a "
            "chance to handle errors "
            "(destination directory must be on the same filesystem of source file)");


    copyok = dispatchOpts->add<StringOption>("copyok", 0, "copyok", "directory",
            "copy the data from input files that was imported successfully to the given directory");
    copyko = dispatchOpts->add<StringOption>("copyko", 0, "copyko", "directory",
            "copy the data from input files that had problems to the given directory");

    ignore_duplicates = dispatchOpts->add<BoolOption>("ignore-duplicates", 0, "ignore-duplicates", "",
            "do not consider the run unsuccessful in case of duplicates");
    validate = dispatchOpts->add<StringOption>("validate", 0, "validate", "checks",
            "run the given checks on the input data before dispatching"
            " (comma-separated list; use 'list' to get a list)");
    dispatch = dispatchOpts->add< VectorOption<String> >("dispatch", 0, "dispatch", "conffile",
            "dispatch the data to the datasets described in the "
            "given configuration file (or a dataset path can also "
            "be given), then output the metadata of the data that "
            "has been dispatched (can be specified multiple times)");
    testdispatch = dispatchOpts->add< VectorOption<String> >("testdispatch", 0, "testdispatch", "conffile",
            "simulate dispatching the files right after scanning, using the given configuration file "
            "or dataset directory (can be specified multiple times)");

    status = dispatchOpts->add<BoolOption>("status", 0, "status", "",
            "print to standard error a line per every file with a summary of how it was handled");

    flush_threshold = dispatchOpts->add<StringOption>("flush-threshold", 0, "flush-threshold", "size",
            "import a batch as soon as the data read so far exceeds this amount of megabytes"
            " (default: 128Mi; use 0 to load all in RAM no matter what)");
}

bool DispatchOptions::handle_parsed_options()
{
    if (dispatch->isSet() && testdispatch->isSet())
        throw commandline::BadOption("you cannot use --dispatch together with --testdispatch");

    if (validate->isSet() && !dispatch_requested())
        throw commandline::BadOption("--validate only makes sense with --dispatch or --testdispatch");

    // Honour --validate=list
    if (validate->stringValue() == "list")
    {
        // Print validator list and exit
        const ValidatorRepository& vals = ValidatorRepository::get();
        for (ValidatorRepository::const_iterator i = vals.begin();
                i != vals.end(); ++i)
            fprintf(stdout, "%s - %s\n", i->second->name.c_str(), i->second->desc.c_str());
        return true;
    }

    if (flush_threshold->isSet())
        try
        {
            parse_size(flush_threshold->stringValue());
        } catch (std::runtime_error& e) {
            throw commandline::BadOption("--flush-threshold wants a number with a valid SI or IEC suffix");
        }

    return false;
}

bool DispatchOptions::dispatch_requested() const
{
    return dispatch->isSet() || testdispatch->isSet();
}


MetadataDispatch::MetadataDispatch(const DispatchOptions& args, DatasetProcessor& next)
    : next(next)
{
    timerclear(&startTime);

    if (args.testdispatch->isSet())
    {
        for (const auto& i: args.testdispatch->values())
            parseConfigFile(cfg, i);
        dispatcher = new TestDispatcher(cfg, cerr);
    }
    else if (args.dispatch->isSet())
    {
        for (const auto& i: args.dispatch->values())
            parseConfigFile(cfg, i);
        dispatcher = new RealDispatcher(cfg);
    }
    else
        throw std::runtime_error("cannot create MetadataDispatch with no --dispatch or --testdispatch information");

    reportStatus = args.status->boolValue();
    if (args.ignore_duplicates && args.ignore_duplicates->boolValue())
        ignore_duplicates = true;

    if (args.validate)
    {
        const ValidatorRepository& vals = ValidatorRepository::get();

        // Add validators to dispatcher
        str::Split splitter(args.validate->stringValue(), ",");
        for (str::Split::const_iterator iname = splitter.begin();
                iname != splitter.end(); ++iname)
        {
            ValidatorRepository::const_iterator i = vals.find(*iname);
            if (i == vals.end())
                throw commandline::BadOption("unknown validator '" + *iname + "'. You can get a list using --validate=list.");
            dispatcher->add_validator(*(i->second));
        }
    }

    if (args.copyok->isSet())
        dir_copyok = args.copyok->stringValue();
    if (args.copyko->isSet())
        dir_copyko = args.copyko->stringValue();

    if (args.flush_threshold->isSet())
        flush_threshold = parse_size(args.flush_threshold->stringValue());
}

MetadataDispatch::~MetadataDispatch()
{
    if (dispatcher)
        delete dispatcher;
}

bool MetadataDispatch::process(dataset::Reader& ds, const std::string& name)
{
    setStartTime();
    results.clear();

    if (!dir_copyok.empty())
        copyok.reset(new core::File(str::joinpath(dir_copyok, str::basename(name))));
    else
        copyok.reset();

    if (!dir_copyko.empty())
        copyko.reset(new core::File(str::joinpath(dir_copyko, str::basename(name))));
    else
        copyko.reset();

    // Read
    try {
        ds.query_data(Matcher(), [&](unique_ptr<Metadata> md) {
            partial_batch_data_size += md->data_size();
            partial_batch.acquire(move(md));
            if (flush_threshold && partial_batch_data_size > flush_threshold)
                process_partial_batch(name);
            return true;
        });
        if (!partial_batch.empty())
            process_partial_batch(name);
    } catch (std::exception& e) {
        nag::warning("%s: cannot read contents: %s", name.c_str(), e.what());
        next.process(results, name);
        throw;
    }

    // Process the resulting annotated metadata as a dataset
    next.process(results, name);

    if (reportStatus)
    {
        cerr << name << ": " << summary_so_far() << endl;
        cerr.flush();
    }

    bool success = !(countNotImported || countInErrorDataset);
    if (ignore_duplicates)
        success = success && (countSuccessful || countDuplicates);
    else
        success = success && (countSuccessful && !countDuplicates);

    flush();

    countSuccessful = 0;
    countNotImported = 0;
    countDuplicates = 0;
    countInErrorDataset = 0;

    return success;
}

void MetadataDispatch::process_partial_batch(const std::string& name)
{
    bool drop_cached_data_on_commit = !(copyok || copyko);

    // Dispatch
    auto batch = partial_batch.make_import_batch();
    try {
        dispatcher->dispatch(batch, drop_cached_data_on_commit);
    } catch (std::exception& e) {
        nag::warning("%s: cannot dispatch contents: %s", name.c_str(), e.what());
        partial_batch.move_to(results.inserter_func());
        throw;
    }

    // Evaluate results
    for (auto& e: batch)
    {
        if (e->dataset_name.empty())
        {
            do_copyko(e->md);
            // If dispatching failed, add a big note about it.
            e->md.add_note("WARNING: The data has not been imported in ANY dataset");
            ++countNotImported;
        } else if (e->dataset_name == "error") {
            do_copyko(e->md);
            ++countInErrorDataset;
        } else if (e->dataset_name == "duplicates") {
            do_copyko(e->md);
            ++countDuplicates;
        } else if (e->result == dataset::ACQ_OK) {
            do_copyok(e->md);
            ++countSuccessful;
        } else {
            do_copyko(e->md);
            // If dispatching failed, add a big note about it.
            e->md.add_note("WARNING: The data failed to be imported into dataset " + e->dataset_name);
            ++countNotImported;
        }
        e->md.drop_cached_data();
    }

    // Process the resulting annotated metadata as a dataset
    partial_batch.move_to(results.inserter_func());
    partial_batch_data_size = 0;
}

void MetadataDispatch::do_copyok(Metadata& md)
{
    if (!copyok)
        return;

    if (!copyok->is_open())
        copyok->open(O_WRONLY | O_APPEND | O_CREAT);

    md.stream_data(*copyok);
}

void MetadataDispatch::do_copyko(Metadata& md)
{
    if (!copyko)
        return;

    if (!copyko->is_open())
        copyko->open(O_WRONLY | O_APPEND | O_CREAT);

    md.stream_data(*copyko);
}

void MetadataDispatch::flush()
{
    if (dispatcher) dispatcher->flush();
}

string MetadataDispatch::summary_so_far() const
{
    string timeinfo;
    if (timerisset(&startTime))
    {
        struct timeval now;
        struct timeval diff;
        gettimeofday(&now, NULL);
        timersub(&now, &startTime, &diff);
        char buf[32];
        snprintf(buf, 32, " in %d.%06d seconds", (int)diff.tv_sec, (int)diff.tv_usec);
        timeinfo = buf;
    }
    if (!countSuccessful && !countNotImported && !countDuplicates && !countInErrorDataset)
        return "no data processed" + timeinfo;

    if (!countNotImported && !countDuplicates && !countInErrorDataset)
    {
        stringstream ss;
        ss << "everything ok: " << countSuccessful << " message";
        if (countSuccessful != 1)
            ss << "s";
        ss << " imported" + timeinfo;
        return ss.str();
    }

    stringstream res;

    if (countNotImported)
        res << "serious problems: ";
    else
        res << "some problems: ";

    res << countSuccessful << " ok, "
        << countDuplicates << " duplicates, "
        << countInErrorDataset << " in error dataset";

    if (countNotImported)
        res << ", " << countNotImported << " NOT imported";

    res << timeinfo;

    return res.str();
}

void MetadataDispatch::setStartTime()
{
    gettimeofday(&startTime, NULL);
}


}
}
