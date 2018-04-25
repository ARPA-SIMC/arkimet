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
}

class Vm2 : public Scanner
{
protected:
    std::istream* in;
    unsigned lineno;

    meteo::vm2::Parser* parser;
    void scan_string(meteo::vm2::Value& value, const std::string& data, Metadata& md);

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
