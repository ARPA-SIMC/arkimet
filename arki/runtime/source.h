#ifndef ARKI_RUNTIME_SOURCE_H
#define ARKI_RUNTIME_SOURCE_H

#include <arki/dataset/fwd.h>
#include <arki/scan/fwd.h>
#include <arki/core/cfg.h>
#include <string>
#include <vector>

namespace arki {
namespace runtime {
struct DatasetProcessor;
struct MetadataDispatch;
struct DispatchOptions;
struct CommandLine;
struct ScanCommandLine;
struct QueryCommandLine;
struct Inputs;


/**
 * Generic interface for data sources configured via the command line
 */
struct Source
{
    Source() {}
    Source(const Source&) = delete;
    Source(Source&&) = delete;
    Source& operator=(const Source&) = delete;
    Source& operator=(Source&&) = delete;
    virtual ~Source();
    virtual std::string name() const = 0;
    virtual dataset::Reader& reader() const = 0;
    virtual void open() = 0;
    virtual void close(bool successful) = 0;
    virtual bool process(DatasetProcessor& processor);
    virtual bool dispatch(MetadataDispatch& dispatcher);
};

/**
 * Data source reading from stdin
 */
struct StdinSource : public Source
{
    scan::Scanner* scanner = nullptr;
    std::shared_ptr<dataset::Reader> m_reader;

    StdinSource(const std::string& format);
    ~StdinSource();

    std::string name() const override;
    dataset::Reader& reader() const override;
    void open() override;
    void close(bool successful) override;
};

/**
 * Data source from the path to a file or dataset
 */
struct FileSource : public Source
{
    core::cfg::Section cfg;
    std::shared_ptr<dataset::Reader> m_reader;
    std::string movework;
    std::string moveok;
    std::string moveko;

    FileSource(const core::cfg::Section& info);
    FileSource(DispatchOptions& args, const core::cfg::Section& info);

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

    MergedSource(const core::cfg::Sections& inputs);

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
    core::cfg::Section cfg;
    std::shared_ptr<dataset::Reader> m_reader;
    std::string m_name;

    QmacroSource(const std::string& macro_name, const std::string& macro_query, const core::cfg::Sections& inputs);

    std::string name() const override;
    dataset::Reader& reader() const override;
    void open() override;
    void close(bool successful) override;
};


/**
 * Instantiate all sources requested on command line.
 *
 * Return true if dest returned true (successful) on all sources.
 */
bool foreach_source(ScanCommandLine& args, const Inputs& inputs, std::function<bool(Source&)> dest);

bool foreach_stdin(const std::string& format, std::function<bool(Source&)> dest);
bool foreach_merged(const core::cfg::Sections& input, std::function<bool(Source&)> dest);
bool foreach_qmacro(const std::string& macro_name, const std::string& macro_query, const core::cfg::Sections& inputs, std::function<bool(Source&)> dest);
bool foreach_sections(const core::cfg::Sections& inputs, std::function<bool(Source&)> dest);

}
}
#endif
