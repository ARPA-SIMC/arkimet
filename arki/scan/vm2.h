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
protected:
    vm2::Input* input = nullptr;
    unsigned lineno;
    bool scan_stream(vm2::Input& in, Metadata& md);

public:
    Vm2();
    virtual ~Vm2();

    void open(const std::string& filename, std::shared_ptr<segment::Reader> reader) override;

    void close() override;
    bool next(Metadata& md) override;
    std::unique_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;

    /// Reconstruct a VM2 based on metadata and a string value
    static std::vector<uint8_t> reconstruct(const Metadata& md, const std::string& value);
};

}
}
#endif
