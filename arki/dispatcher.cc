#include "dispatcher.h"
#include "configfile.h"
#include "metadata/consumer.h"
#include "matcher.h"
#include "dataset.h"
#include "validator.h"
#include "types/reftime.h"
#include "utils/string.h"
#include "utils/sys.h"

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {

static inline Matcher getFilter(const ConfigFile* cfg)
{
    try {
        return Matcher::parse(cfg->value("filter"));
    } catch (std::runtime_error& e) {
        const ConfigFile::FilePos* fp = cfg->valueInfo("filter");
        if (fp)
        {
            stringstream ss;
            ss << fp->pathname << ":" << fp->lineno << ": " << e.what();
            throw std::runtime_error(ss.str());
        }
        throw;
    }
}

Dispatcher::Dispatcher(const ConfigFile& cfg)
    : m_can_continue(true), m_outbound_failures(0)
{
    // Validate the configuration, and split normal datasets from outbound
    // datasets
    for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
            i != cfg.sectionEnd(); ++i)
    {
        if (i->first == "error" or i->first == "duplicates")
            continue;
        else if (i->second->value("type") == "outbound")
        {
            if (i->second->value("filter").empty())
                throw std::runtime_error("configuration of dataset '"+i->first+"' does not have a 'filter' directive");
            outbounds.push_back(make_pair(i->first, getFilter(i->second)));
        }
        else {
            if (i->second->value("filter").empty())
                throw std::runtime_error("configuration of dataset '"+i->first+"' does not have a 'filter' directive");
            datasets.push_back(make_pair(i->first, getFilter(i->second)));
        }
    }
}

Dispatcher::~Dispatcher()
{
}

void Dispatcher::add_validator(const Validator& v)
{
    validators.push_back(&v);
}

void Dispatcher::hook_pre_dispatch(const Metadata& md)
{
}

void Dispatcher::hook_found_datasets(const Metadata& md, vector<string>& found)
{
}

void Dispatcher::hook_output(unique_ptr<Metadata> md, metadata_dest_func mdc)
{
}

dataset::Writer::AcquireResult Dispatcher::raw_dispatch_error(Metadata& md) { return raw_dispatch_dataset("error", md); }
dataset::Writer::AcquireResult Dispatcher::raw_dispatch_duplicates(Metadata& md) { return raw_dispatch_dataset("duplicates", md); }

Dispatcher::Outcome Dispatcher::dispatch(unique_ptr<Metadata>&& md, metadata_dest_func mdc)
{
    Dispatcher::Outcome result;
    vector<string> found;

    hook_pre_dispatch(*md);

    // Ensure that we have a reference time
    const reftime::Position* rt = md->get<types::reftime::Position>();
    if (!rt)
    {
        using namespace arki::types;
        md->add_note("Validation error: reference time is missing");
        // Set today as a dummy reference time, and import into the error dataset
        md->set(Reftime::createPosition(Time::createNow()));
        result = raw_dispatch_error(*md) == dataset::Writer::ACQ_OK ? DISP_ERROR : DISP_NOTWRITTEN;
        goto done;
    }

    // Run validation
    if (!validators.empty())
    {
        bool validates_ok = true;
        vector<string> errors;

        // Run validators
        for (vector<const Validator*>::const_iterator i = validators.begin();
                i != validators.end(); ++i)
            validates_ok = validates_ok && (**i)(*md, errors);

        // Annotate with the validation errors
        for (vector<string>::const_iterator i = errors.begin();
                i != errors.end(); ++i)
            md->add_note("Validation error: " + *i);

        if (!validates_ok)
        {
            // Dispatch directly to the error dataset
            result = raw_dispatch_error(*md) == dataset::Writer::ACQ_OK ? DISP_ERROR : DISP_NOTWRITTEN;
            goto done;
        }
    }


    // Fetch the data into memory here, so that if problems arise we do not
    // fail in bits of code that are more critical
    try {
        md->getData();
    } catch (std::exception& e) {
        md->add_note(string("Failed to read the data associated with the metadata: ") + e.what());
        result = DISP_NOTWRITTEN;
        goto done;
    }

    // See what outbound datasets match this metadata
    for (vector< pair<string, Matcher> >::const_iterator i = outbounds.begin();
            i != outbounds.end(); ++i)
        if (i->second(*md))
        {
            // Operate on a copy
            unique_ptr<Metadata> md1(new Metadata(*md));
            // File it to the outbound dataset right away
            if (raw_dispatch_dataset(i->first, *md1) != dataset::Writer::ACQ_OK)
            {
                // What do we do in case of error?
                // The dataset will already have added a note to the dataset
                // explaining what was wrong.  The best we can do is keep a
                // count of failures.
                ++m_outbound_failures;
            }
            if (m_can_continue)
                m_can_continue = mdc(move(md1));
        }

    // See how many proper datasets match this metadata
    for (vector< pair<string, Matcher> >::const_iterator i = datasets.begin();
            i != datasets.end(); ++i)
        if (i->second(*md))
            found.push_back(i->first);
    hook_found_datasets(*md, found);

    // If we found only one dataset, acquire it in that dataset; else,
    // acquire it in the error dataset
    if (found.empty())
    {
        md->add_note("Message could not be assigned to any dataset");
        result = raw_dispatch_error(*md) == dataset::Writer::ACQ_OK ? DISP_ERROR : DISP_NOTWRITTEN;
        goto done;
    }

    if (found.size() > 1)
    {
        string msg = "Message matched multiple datasets: ";
        for (vector<string>::const_iterator i = found.begin();
                i != found.end(); ++i)
            if (i == found.begin())
                msg += *i;
            else
                msg += ", " + *i;
        md->add_note(msg);
        result = raw_dispatch_error(*md) == dataset::Writer::ACQ_OK ? DISP_ERROR : DISP_NOTWRITTEN;
        goto done;
    }

    // Acquire into the dataset
    switch (raw_dispatch_dataset(found[0], *md))
    {
        case dataset::Writer::ACQ_OK:
            result = DISP_OK;
            break;
        case dataset::Writer::ACQ_ERROR_DUPLICATE:
            // If insertion in the designed dataset failed, insert in the
            // error dataset
            result = raw_dispatch_duplicates(*md) == dataset::Writer::ACQ_OK ? DISP_DUPLICATE_ERROR : DISP_NOTWRITTEN;
            break;
        case dataset::Writer::ACQ_ERROR:
        default:
            // If insertion in the designed dataset failed, insert in the
            // error dataset
            result = raw_dispatch_error(*md) == dataset::Writer::ACQ_OK ? DISP_ERROR : DISP_NOTWRITTEN;
            break;
    }

done:
    hook_output(move(md), mdc);
    return result;
}


RealDispatcher::RealDispatcher(const ConfigFile& cfg)
    : Dispatcher(cfg), pool(cfg), dserror(0), dsduplicates(0)
{
    // Instantiate the error dataset in the cache
    dserror = pool.get("error");
    if (!dserror)
        throw std::runtime_error("no [error] dataset found");

    // Instantiate the duplicates dataset in the cache
    dsduplicates = pool.get("duplicates");
}

RealDispatcher::~RealDispatcher()
{
	// The error and duplicates datasets do not want deallocation, as they are
	// a reference to the version inside the DatasetPool cache
}

void RealDispatcher::hook_output(unique_ptr<Metadata> md, metadata_dest_func mdc)
{
    if (m_can_continue)
        m_can_continue = mdc(move(md));
}

dataset::Writer::AcquireResult RealDispatcher::raw_dispatch_dataset(const std::string& name, Metadata& md)
{
    // File it to the outbound dataset right away
    dataset::Writer* target = pool.get(name);
    return target->acquire(md);
}

dataset::Writer::AcquireResult RealDispatcher::raw_dispatch_error(Metadata& md)
{
    return dserror->acquire(md);
}

dataset::Writer::AcquireResult RealDispatcher::raw_dispatch_duplicates(Metadata& md)
{
    dataset::Writer* target = dsduplicates ? dsduplicates : dserror;
    return target->acquire(md);
}

void RealDispatcher::flush() { pool.flush(); }



TestDispatcher::TestDispatcher(const ConfigFile& cfg, std::ostream& out)
    : Dispatcher(cfg), cfg(cfg), out(out), m_count(0)
{
    if (!cfg.section("error"))
        throw std::runtime_error("no [error] dataset found");
}
TestDispatcher::~TestDispatcher() {}

void TestDispatcher::hook_pre_dispatch(const Metadata& md)
{
    // Increment the metadata counter, so that we can refer to metadata in the
    // messages
    ++m_count;
	stringstream out;
	out << "Message " << md.source();
    prefix = out.str().c_str();
}

void TestDispatcher::hook_found_datasets(const Metadata& md, vector<string>& found)
{
    if (found.empty())
    {
        out << prefix << ": not matched by any dataset" << endl;
    }
    else if (found.size() > 1)
    {
        out << prefix << ": matched by multiple datasets: ";
        for (vector<string>::const_iterator i = found.begin();
                i != found.end(); ++i)
            if (i == found.begin())
                out << *i;
            else
                out << ", " << *i;
        out << endl;
    }
}

dataset::Writer::AcquireResult TestDispatcher::raw_dispatch_dataset(const std::string& name, Metadata& md)
{
    out << prefix << ": acquire to " << name << " dataset" << endl;
    return dataset::Writer::testAcquire(*cfg.section(name), md, out);
}

void TestDispatcher::flush()
{
	// Reset the metadata counter
	m_count = 0;
}

}
