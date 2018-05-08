#include "dispatch.h"
#include "processor.h"
#include "arki/runtime.h"
#include "arki/dispatcher.h"
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

MetadataDispatch::MetadataDispatch(const ScanCommandLine& args, DatasetProcessor& next)
    : next(next)
{
    timerclear(&startTime);

    runtime::readMatcherAliasDatabase();

    if (args.testdispatch && args.testdispatch->isSet())
    {
        for (const auto& i: args.testdispatch->values())
            parseConfigFile(cfg, i);
        dispatcher = new TestDispatcher(cfg, cerr);
    }
    else if (args.dispatch && args.dispatch->isSet())
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
        copyok.reset(new core::File(str::joinpath(dir_copyok, str::basename(name)), O_WRONLY | O_APPEND | O_CREAT));
    else
        copyok.reset();

    if (!dir_copyko.empty())
        copyko.reset(new core::File(str::joinpath(dir_copyko, str::basename(name)), O_WRONLY | O_APPEND | O_CREAT));
    else
        copyko.reset();

    // Read
    try {
        ds.query_data(Matcher(), results.inserter_func());
    } catch (std::exception& e) {
        nag::warning("%s: cannot read contents: %s", name.c_str(), e.what());
        next.process(results, name);
        throw;
    }

    // Dispatch
    auto batch = results.make_import_batch();
    try {
        dispatcher->dispatch(batch);
    } catch (std::exception& e) {
        nag::warning("%s: cannot dispatch contents: %s", name.c_str(), e.what());
        next.process(results, name);
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
    }

    // Process the resulting annotated metadata as a dataset
    next.process(results, name);

    if (reportStatus)
    {
        cerr << name << ": " << summarySoFar() << endl;
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

void MetadataDispatch::do_copyok(Metadata& md)
{
    //fprintf(stderr, "ZAZA: %s\n", md.sourceBlob().filename.c_str());
    if (copyok && copyok->is_open())
        md.stream_data(*copyok);
}

void MetadataDispatch::do_copyko(Metadata& md)
{
    if (copyko && copyko->is_open())
        md.stream_data(*copyko);
}

void MetadataDispatch::flush()
{
    if (dispatcher) dispatcher->flush();
}

string MetadataDispatch::summarySoFar() const
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
