#ifndef ARKI_RUNTIME_PROCESSOR_H
#define ARKI_RUNTIME_PROCESSOR_H

/// Run user requested operations on datasets

#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
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

namespace python {
namespace cmdline {

struct DatasetProcessor
{
    virtual ~DatasetProcessor() {}

    virtual void process(dataset::Reader& ds, const std::string& name) = 0;
    virtual void end() {}
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
    std::string archive;
    std::string summary_restrict;
    std::string sort;
    std::shared_ptr<arki::dataset::QueryProgress> progress;

    /// Create the processor maker for this configuration
    std::unique_ptr<DatasetProcessor> make(Matcher matcher, std::shared_ptr<core::NamedFileDescriptor> out);
    std::unique_ptr<DatasetProcessor> make(Matcher matcher, std::shared_ptr<core::AbstractOutputFile> out);

protected:
    template<typename Output>
    std::unique_ptr<DatasetProcessor> make_binary(Matcher matcher, std::shared_ptr<Output> out);
    template<typename Output>
    std::unique_ptr<DatasetProcessor> make_summary(Matcher matcher, std::shared_ptr<Output> out);
    template<typename Output>
    std::unique_ptr<DatasetProcessor> make_metadata(Matcher matcher, std::shared_ptr<Output> out);

};

}
}
}
#endif
