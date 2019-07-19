#ifndef ARKI_RUNTIME_ARKISCAN_H
#define ARKI_RUNTIME_ARKISCAN_H

#include <arki/core/cfg.h>
#include <arki/runtime/processor.h>
#include <arki/runtime/dispatch.h>
#include <arki/runtime/source.h>

namespace arki {
namespace runtime {

class ArkiScan
{
public:
    core::cfg::Sections inputs;
    DatasetProcessor* processor = nullptr;
    MetadataDispatch* dispatcher = nullptr;

    ArkiScan() = default;
    ~ArkiScan();
    ArkiScan(const ArkiScan&) = delete;
    ArkiScan(ArkiScan&&) = delete;
    ArkiScan& operator=(const ArkiScan&) = delete;
    ArkiScan& operator=(ArkiScan&&) = delete;

    std::function<bool(runtime::Source& source)> make_dest_func(bool& dispatch_ok);

    bool run_scan_stdin(const std::string& format);
    bool run_scan_inputs(const std::string& moveok, const std::string& moveko, const std::string& movework);

    int run(int argc, const char* argv[]);
};

}
}

#endif

