#ifndef ARKI_RUNTIME_SOURCE_H
#define ARKI_RUNTIME_SOURCE_H

#include <arki/dataset/fwd.h>
#include <arki/configfile.h>
#include <string>
#include <vector>

namespace arki {
namespace runtime {
struct DatasetProcessor;
struct MetadataDispatch;
struct CommandLine;
struct ScanCommandLine;


/**
 * Generic interface for data sources configured via the command line
 */
struct Source
{
    virtual ~Source();
    virtual std::string name() const = 0;
    virtual dataset::Reader& reader() const = 0;
    virtual void open() = 0;
    virtual void close(bool successful) = 0;
    virtual bool process(DatasetProcessor& processor);
    virtual bool dispatch(MetadataDispatch& dispatcher);
};

/**
 * Data source from the path to a file or dataset
 */
struct FileSource : public Source
{
    ConfigFile cfg;
    std::shared_ptr<dataset::Reader> m_reader;
    std::string movework;
    std::string moveok;
    std::string moveko;

    FileSource(CommandLine& args, const ConfigFile& info);
    FileSource(ScanCommandLine& args, const ConfigFile& info);

    std::string name() const override;
    dataset::Reader& reader() const override;
    void open() override;
    void close(bool successful) override;
};

/**
 * Data source from --merged
 */
struct MergedSource : public Source
{
    std::vector<std::shared_ptr<FileSource>> sources;
    std::shared_ptr<dataset::Merged> m_reader;
    std::string m_name;

    MergedSource(CommandLine& args);

    std::string name() const override;
    dataset::Reader& reader() const override;
    void open() override;
    void close(bool successful) override;
};

/**
 * Data source from --qmacro
 */
struct QmacroSource : public Source
{
    ConfigFile cfg;
    std::shared_ptr<dataset::Reader> m_reader;
    std::string m_name;

    QmacroSource(CommandLine& args);

    std::string name() const override;
    dataset::Reader& reader() const override;
    void open() override;
    void close(bool successful) override;
};


}
}
#endif
