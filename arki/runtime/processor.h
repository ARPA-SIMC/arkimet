#ifndef ARKI_RUNTIME_PROCESSOR_H
#define ARKI_RUNTIME_PROCESSOR_H

/// Run user requested operations on datasets

#include <arki/core/fwd.h>
#include <arki/utils/sys.h>
#include <string>
#include <memory>
#include <vector>
#include <functional>

namespace arki {
class Matcher;

namespace dataset {
class Reader;
}

namespace runtime {
struct ScanCommandLine;
struct QueryCommandLine;

struct DatasetProcessor
{
    virtual ~DatasetProcessor() {}

    virtual void process(dataset::Reader& ds, const std::string& name) = 0;
    virtual void end() {}
    virtual std::string describe() const = 0;
};

struct SingleOutputProcessor : public DatasetProcessor
{
    utils::sys::NamedFileDescriptor output;

    SingleOutputProcessor();
    SingleOutputProcessor(const utils::sys::NamedFileDescriptor& out);
};

struct TargetFileProcessor : public DatasetProcessor
{
    SingleOutputProcessor* next;
    std::string pattern;
    std::vector<std::string> description_attrs;

    TargetFileProcessor(SingleOutputProcessor* next, const std::string& pattern);
    virtual ~TargetFileProcessor();

    virtual std::string describe() const;
    virtual void process(dataset::Reader& ds, const std::string& name);
    virtual void end();
};

struct ProcessorMaker
{
    bool summary = false;
    bool summary_short = false;
    bool yaml = false;
    bool json = false;
    bool annotate = false;
    bool data_only = false;
    bool data_inline = false;
    // True if we are running in arki-server and we are running the server side
    // of a remote query
    bool server_side = false;
    std::string postprocess;
    std::string report;
    std::string archive;
    std::string summary_restrict;
    std::string sort;

    /// Create the processor maker for this configuration
    std::unique_ptr<DatasetProcessor> make(Matcher query, utils::sys::NamedFileDescriptor& out);
};

namespace processor {

void verify_option_consistency(ScanCommandLine& args);
void verify_option_consistency(QueryCommandLine& args);

std::unique_ptr<DatasetProcessor> create(ScanCommandLine& args, core::NamedFileDescriptor& output);
std::unique_ptr<DatasetProcessor> create(QueryCommandLine& args, const Matcher& query, core::NamedFileDescriptor& output);

}


}
}
#endif
