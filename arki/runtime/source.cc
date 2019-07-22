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


FileSource::FileSource(const core::cfg::Section& info)
    : cfg(info)
{
}

void FileSource::open()
{
    if (!movework.empty() && cfg.value("type") == "file")
        cfg.set("path", moveFile(cfg.value("path"), movework));
    reader = dataset::Reader::create(cfg);
}

void FileSource::close(bool successful)
{
    if (successful && !moveok.empty())
        moveFile(*reader, moveok);
    else if (!successful && !moveko.empty())
        moveFile(*reader, moveko);
    reader = std::shared_ptr<dataset::Reader>();
}



bool foreach_stdin(const std::string& format, std::function<void(dataset::Reader&)> dest)
{
    auto scanner = scan::Scanner::get_scanner(format);

    core::cfg::Section cfg;
    cfg.set("format", format);
    cfg.set("name", "stdin:" + scanner->name());
    auto config = dataset::fromfunction::Config::create(cfg);

    auto reader = std::make_shared<dataset::fromfunction::Reader>(config);
    reader->generator = [&](metadata_dest_func dest){
        sys::NamedFileDescriptor fd_stdin(0, "stdin");
        return scanner->scan_pipe(fd_stdin, dest);
    };

    bool success = true;
    try {
        dest(*reader);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", reader->name().c_str(), e.what());
        success = false;
    }
    return success;
}

bool foreach_merged(const core::cfg::Sections& input, std::function<void(dataset::Reader&)> dest)
{
    bool success = true;
    std::shared_ptr<dataset::Merged> reader = std::make_shared<dataset::Merged>();

    std::vector<std::shared_ptr<FileSource>> sources;
    for (auto si: input)
        sources.push_back(std::make_shared<FileSource>(si.second));

    // Instantiate the datasets and add them to the merger
    for (const auto& source: sources)
    {
        source->open();
        reader->add_dataset(source->reader);
    }

    try {
        dest(*reader);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", reader->name().c_str(), e.what());
        success = false;
    }

    for (auto& source: sources)
        source->close(success);

    return success;
}

bool foreach_qmacro(const std::string& macro_name, const std::string& macro_query, const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest)
{
    bool success = true;

    core::cfg::Section cfg;
    std::shared_ptr<dataset::Reader> reader;

    // Create the virtual qmacro dataset
    std::string baseurl = dataset::http::Reader::allSameRemoteServer(inputs);
    if (baseurl.empty())
    {
        // Create the local query macro
        nag::verbose("Running query macro %s on local datasets", macro_name.c_str());
        reader = std::make_shared<Querymacro>(cfg, inputs, macro_name, macro_query);
    } else {
        // Create the remote query macro
        nag::verbose("Running query macro %s on %s", macro_name.c_str(), baseurl.c_str());
        core::cfg::Section cfg;
        cfg.set("name", macro_name);
        cfg.set("type", "remote");
        cfg.set("path", baseurl);
        cfg.set("qmacro", macro_query);
        reader = dataset::Reader::create(cfg);
    }

    try {
        dest(*reader);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", reader->name().c_str(), e.what());
        success = false;
    }
    return success;
}

bool foreach_sections(const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest)
{
    bool all_successful = true;
    // Query all the datasets in sequence
    for (auto si: inputs)
    {
        FileSource source(si.second);
        source.open();
        bool success = true;
        try {
            dest(*source.reader);
        } catch (std::exception& e) {
            nag::warning("%s failed: %s", source.reader->name().c_str(), e.what());
            success = false;
        }
        source.close(success);
        if (!success) all_successful = false;
    }
    return all_successful;
}

}
}
