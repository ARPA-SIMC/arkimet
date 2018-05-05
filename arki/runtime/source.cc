#include "source.h"
#include "processor.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"
#include "arki/exceptions.h"
#include "arki/dataset/file.h"
#include "arki/dataset/merged.h"

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


FileSource::FileSource(CommandLine& args, const ConfigFile& info)
    : cfg(info)
{
}

FileSource::FileSource(ScanCommandLine& args, const ConfigFile& info)
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

MergedSource::MergedSource(CommandLine& args)
    : m_reader(std::make_shared<dataset::Merged>())
{
    for (const auto& cfg: args.inputs)
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


QmacroSource::QmacroSource(CommandLine& args)
{
    // Create the virtual qmacro dataset
    m_reader = runtime::make_qmacro_dataset(
            cfg,
            args.inputs.as_config(),
            args.qmacro->stringValue(),
            args.strquery);
    m_name = args.qmacro->stringValue();
}

std::string QmacroSource::name() const { return m_name; }
dataset::Reader& QmacroSource::reader() const { return *m_reader; }
void QmacroSource::open() {}
void QmacroSource::close(bool successful) {}



}
}
