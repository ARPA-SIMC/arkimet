#ifndef ARKI_SCAN_ODIMH5_H
#define ARKI_SCAN_ODIMH5_H

#include <arki/scan.h>
#include <string>
#include <vector>

struct lua_State;

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace odimh5 {
const Validator& validator();
}

class OdimScanner : public Scanner
{
    void set_blob_source(Metadata& md, std::shared_ptr<segment::Reader> reader);

protected:
    virtual std::shared_ptr<Metadata> scan_h5_file(const std::string& pathname) = 0;
    virtual std::shared_ptr<Metadata> scan_h5_data(const std::vector<uint8_t>& data);

public:
    std::string name() const override { return "odimh5"; }

    std::shared_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    std::shared_ptr<Metadata> scan_singleton(const std::string& abspath) override;
};


void register_odimh5_lua();

}
}

#endif
