#ifndef ARKI_SCAN_VM2_H
#define ARKI_SCAN_VM2_H

/// Scan a VM2 file for metadata

#include <arki/scan.h>
#include <string>
#include <vector>
#include <unistd.h>

namespace meteo {
namespace vm2 {
class Parser;
class Value;
}
}

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace vm2 {
const Validator& validator();

struct Input;
}

class Vm2 : public Scanner
{
public:
    Vm2();
    virtual ~Vm2();

    std::string name() const override { return "vm2"; }
    std::shared_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    std::shared_ptr<Metadata> scan_singleton(const std::string& abspath) override;

    /// Reconstruct a VM2 based on metadata and a string value
    static std::vector<uint8_t> reconstruct(const Metadata& md, const std::string& value);
};

}
}
#endif
