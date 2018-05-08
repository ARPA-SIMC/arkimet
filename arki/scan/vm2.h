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
    std::string filename;
    std::shared_ptr<segment::Reader> reader;
    vm2::Input* input = nullptr;
    unsigned lineno;
    bool scan_stream(vm2::Input& in, Metadata& md);
    bool next(Metadata& md);
    void open(const std::string& filename, std::shared_ptr<segment::Reader> reader);
    void close();

public:
    Vm2();
    virtual ~Vm2();

    std::unique_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_file(const std::string& abspath, std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    bool scan_file_inline(const std::string& abspath, metadata_dest_func dest) override;

    /// Reconstruct a VM2 based on metadata and a string value
    static std::vector<uint8_t> reconstruct(const Metadata& md, const std::string& value);
};

}
}
#endif
