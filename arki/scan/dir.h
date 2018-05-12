#ifndef ARKI_SCAN_DIR_H
#define ARKI_SCAN_DIR_H

#include <arki/scan.h>

namespace arki {
class Metadata;

namespace scan {
struct Validator;

class Dir : public Scanner
{
protected:

public:
    Dir();
    virtual ~Dir();

    std::string name() const override { return "dir"; }
    std::unique_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    size_t scan_singleton(const std::string& abspath, Metadata& md) override;
};

}
}
#endif

