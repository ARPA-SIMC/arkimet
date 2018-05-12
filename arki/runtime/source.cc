#include "source.h"
#include "processor.h"
#include "dispatch.h"
#include "inputs.h"
#include "arki/runtime.h"
#include "arki/scan.h"
#include "arki/utils/string.h"
#include "arki/exceptions.h"
#include "arki/dataset/file.h"
#include "arki/dataset/http.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/fromfunction.h"
#include "arki/querymacro.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"

using namespace arki;
using namespace arki::utils;

namespace arki {
namespace runtime {

static std::string moveFile(const std::string& source, const std::string& targetdir)
{
    std::string targetFile = str::joinpath(targetdir, str::basename(source));
    if (::rename(source.c_str(), targetFile.c_str()) == -1)
        throw_system_error ("cannot move " + source + " to " + targetFile);
    return targetFile;
}

static std::string moveFile(const dataset::Reader& ds, const std::string& targetdir)
{
    if (const dataset::File* d = dynamic_cast<const dataset::File*>(&ds))
        return moveFile(d->config().pathname, targetdir);
    else
        return std::string();
}

Source::~Source() {}

bool Source::process(DatasetProcessor& processor)
{
    processor.process(reader(), name());
    return true;
}

bool Source::dispatch(MetadataDispatch& dispatcher)
{
    return dispatcher.process(reader(), name());
}


StdinSource::StdinSource(CommandLine& args, const std::string& format)
    : scanner(scan::Scanner::get_scanner(format).release())
{
    ConfigFile cfg;
    cfg.setValue("format", format);
    auto config = dataset::fromfunction::Config::create(cfg);
    auto reader = config->create_reader();
    m_reader = std::make_shared<dataset::fromfunction::Reader>(config);
    // TODO: pass function to constructor
    auto ff_reader = std::dynamic_pointer_cast<dataset::fromfunction::Reader>(m_reader);
    ff_reader->generator = [&](metadata_dest_func dest){
        auto scanner = scan::Scanner::get_scanner(format);
        sys::NamedFileDescriptor fd_stdin(0, "stdin");
        return scanner->scan_pipe(fd_stdin, dest);
    };
}

StdinSource::~StdinSource()
{
    delete scanner;
}

std::string StdinSource::name() const { return "stdin:" + scanner->name(); }
dataset::Reader& StdinSource::reader() const { return *m_reader; }
void StdinSource::open() {}
void StdinSource::close(bool successful) {}


FileSource::FileSource(QueryCommandLine& args, const ConfigFile& info)
    : cfg(info)
{
}

FileSource::FileSource(DispatchOptions& args, const ConfigFile& info)
    : cfg(info)
{
    if (args.movework && args.movework->isSet())
        movework = args.movework->stringValue();
    if (args.moveok && args.moveok->isSet())
        moveok = args.moveok->stringValue();
    if (args.moveko && args.moveko->isSet())
        moveko = args.moveko->stringValue();
}

std::string FileSource::name() const { return cfg.value("path"); }
dataset::Reader& FileSource::reader() const { return *m_reader; }

void FileSource::open()
{
    if (!movework.empty() && cfg.value("type") == "file")
        cfg.setValue("path", moveFile(cfg.value("path"), movework));
    m_reader = dataset::Reader::create(cfg);
}

void FileSource::close(bool successful)
{
    if (successful && !moveok.empty())
        moveFile(reader(), moveok);
    else if (!successful && !moveko.empty())
        moveFile(reader(), moveko);
    m_reader = std::shared_ptr<dataset::Reader>();
}

MergedSource::MergedSource(QueryCommandLine& args, const Inputs& inputs)
    : m_reader(std::make_shared<dataset::Merged>())
{
    for (const auto& cfg: inputs)
    {
        sources.push_back(std::make_shared<FileSource>(args, cfg));
        if (m_name.empty())
            m_name = sources.back()->name();
        else
            m_name += "," + sources.back()->name();
    }
}

std::string MergedSource::name() const { return m_name; }
dataset::Reader& MergedSource::reader() const { return *m_reader; }

void MergedSource::open()
{
    // Instantiate the datasets and add them to the merger
    for (const auto& source: sources)
    {
        source->open();
        m_reader->add_dataset(source->m_reader);
    }
}

void MergedSource::close(bool successful)
{
    for (auto& source: sources)
        source->close(successful);
}


QmacroSource::QmacroSource(QueryCommandLine& args, const Inputs& inputs)
{
    // Create the virtual qmacro dataset
    ConfigFile inputs_cfg = inputs.as_config();
    std::string baseurl = dataset::http::Reader::allSameRemoteServer(inputs_cfg);
    if (baseurl.empty())
    {
        // Create the local query macro
        nag::verbose("Running query macro %s on local datasets", args.qmacro->stringValue().c_str());
        m_reader = std::make_shared<Querymacro>(cfg, inputs_cfg, args.qmacro->stringValue(), args.qmacro_query);
    } else {
        // Create the remote query macro
        nag::verbose("Running query macro %s on %s", args.qmacro->stringValue().c_str(), baseurl.c_str());
        ConfigFile cfg;
        cfg.setValue("name", args.qmacro->stringValue());
        cfg.setValue("type", "remote");
        cfg.setValue("path", baseurl);
        cfg.setValue("qmacro", args.qmacro_query);
        m_reader = dataset::Reader::create(cfg);
    }

    m_name = args.qmacro->stringValue();
}

std::string QmacroSource::name() const { return m_name; }
dataset::Reader& QmacroSource::reader() const { return *m_reader; }
void QmacroSource::open() {}
void QmacroSource::close(bool successful) {}



bool foreach_source(ScanCommandLine& args, const Inputs& inputs, std::function<bool(Source&)> dest)
{
    bool all_successful = true;

    if (args.stdin_input->isSet())
    {
        StdinSource source(args, args.stdin_input->stringValue());
        source.open();
        try {
            all_successful = dest(source);
        } catch (std::exception& e) {
            nag::warning("%s failed: %s", source.name().c_str(), e.what());
            all_successful = false;
        }
        source.close(all_successful);
    } else {
        // Query all the datasets in sequence
        for (const auto& cfg: inputs)
        {
            FileSource source(*args.dispatch_options, cfg);
            nag::verbose("Processing %s...", source.name().c_str());
            source.open();
            bool success;
            try {
                success = dest(source);
            } catch (std::exception& e) {
                nag::warning("%s failed: %s", source.name().c_str(), e.what());
                success = false;
            }
            source.close(success);
            if (!success) all_successful = false;
        }
    }

    return all_successful;
}

bool foreach_source(QueryCommandLine& args, const Inputs& inputs, std::function<bool(Source&)> dest)
{
    bool all_successful = true;

    if (args.stdin_input->isSet())
    {
        StdinSource source(args, args.stdin_input->stringValue());
        source.open();
        try {
            all_successful = dest(source);
        } catch (std::exception& e) {
            nag::warning("%s failed: %s", source.name().c_str(), e.what());
            all_successful = false;
        }
        source.close(all_successful);
    } else {
        if (args.merged->boolValue())
        {
            MergedSource source(args, inputs);
            nag::verbose("Processing %s...", source.name().c_str());
            source.open();
            try {
                all_successful = dest(source);
            } catch (std::exception& e) {
                nag::warning("%s failed: %s", source.name().c_str(), e.what());
                all_successful = false;
            }
            source.close(all_successful);
        } else if (args.qmacro->isSet()) {
            QmacroSource source(args, inputs);
            nag::verbose("Processing %s...", source.name().c_str());
            source.open();
            try {
                all_successful = dest(source);
            } catch (std::exception& e) {
                nag::warning("%s failed: %s", source.name().c_str(), e.what());
                all_successful = false;
            }
            source.close(all_successful);
        } else {
            // Query all the datasets in sequence
            for (const auto& cfg: inputs)
            {
                FileSource source(args, cfg);
                nag::verbose("Processing %s...", source.name().c_str());
                source.open();
                bool success;
                try {
                    success = dest(source);
                } catch (std::exception& e) {
                    nag::warning("%s failed: %s", source.name().c_str(), e.what());
                    success = false;
                }
                source.close(success);
                if (!success) all_successful = false;
            }
        }
    }

    return all_successful;
}




}
}
