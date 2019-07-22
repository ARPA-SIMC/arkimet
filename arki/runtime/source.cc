#include "source.h"
#include "processor.h"
#include "dispatch.h"
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


StdinSource::StdinSource(const std::string& format)
    : scanner(scan::Scanner::get_scanner(format).release())
{
    core::cfg::Section cfg;
    cfg.set("format", format);
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


FileSource::FileSource(const core::cfg::Section& info)
    : cfg(info)
{
}

std::string FileSource::name() const { return cfg.value("path"); }
dataset::Reader& FileSource::reader() const { return *m_reader; }

void FileSource::open()
{
    if (!movework.empty() && cfg.value("type") == "file")
        cfg.set("path", moveFile(cfg.value("path"), movework));
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


MergedSource::MergedSource(const core::cfg::Sections& inputs)
    : m_reader(std::make_shared<dataset::Merged>())
{
    for (auto si: inputs)
    {
        sources.push_back(std::make_shared<FileSource>(si.second));
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


QmacroSource::QmacroSource(const std::string& macro_name, const std::string& macro_query, const core::cfg::Sections& inputs)
{
    // Create the virtual qmacro dataset
    std::string baseurl = dataset::http::Reader::allSameRemoteServer(inputs);
    if (baseurl.empty())
    {
        // Create the local query macro
        nag::verbose("Running query macro %s on local datasets", macro_name.c_str());
        m_reader = std::make_shared<Querymacro>(cfg, inputs, macro_name, macro_query);
    } else {
        // Create the remote query macro
        nag::verbose("Running query macro %s on %s", macro_name.c_str(), baseurl.c_str());
        core::cfg::Section cfg;
        cfg.set("name", macro_name);
        cfg.set("type", "remote");
        cfg.set("path", baseurl);
        cfg.set("qmacro", macro_query);
        m_reader = dataset::Reader::create(cfg);
    }

    m_name = macro_name;
}

std::string QmacroSource::name() const { return m_name; }
dataset::Reader& QmacroSource::reader() const { return *m_reader; }
void QmacroSource::open() {}
void QmacroSource::close(bool successful) {}

bool foreach_stdin(const std::string& format, std::function<void(Source&)> dest)
{
    bool success = true;
    StdinSource source(format);
    source.open();
    try {
        dest(source);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", source.name().c_str(), e.what());
        success = false;
    }
    source.close(success);
    return success;
}

bool foreach_merged(const core::cfg::Sections& input, std::function<void(Source&)> dest)
{
    bool success = true;
    MergedSource source(input);
    nag::verbose("Processing %s...", source.name().c_str());
    source.open();
    try {
        dest(source);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", source.name().c_str(), e.what());
        success = false;
    }
    source.close(success);
    return success;
}

bool foreach_qmacro(const std::string& macro_name, const std::string& macro_query, const core::cfg::Sections& inputs, std::function<void(Source&)> dest)
{
    bool success = true;
    QmacroSource source(macro_name, macro_query, inputs);
    nag::verbose("Processing %s...", source.name().c_str());
    source.open();
    try {
        dest(source);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", source.name().c_str(), e.what());
        success = false;
    }
    source.close(success);
    return success;
}

bool foreach_sections(const core::cfg::Sections& inputs, std::function<void(Source&)> dest)
{
    bool all_successful = true;
    // Query all the datasets in sequence
    for (auto si: inputs)
    {
        FileSource source(si.second);
        nag::verbose("Processing %s...", source.name().c_str());
        source.open();
        bool success = true;
        try {
            dest(source);
        } catch (std::exception& e) {
            nag::warning("%s failed: %s", source.name().c_str(), e.what());
            success = false;
        }
        source.close(success);
        if (!success) all_successful = false;
    }
    return all_successful;
}

}
}
