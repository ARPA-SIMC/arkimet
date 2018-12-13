#ifndef ARKI_SCAN_GRIB_H
#define ARKI_SCAN_GRIB_H

/// scan/grib - Scan a GRIB (version 1 or 2) file for metadata

#include <arki/scan.h>
#include <string>
#include <vector>

struct grib_context;
struct grib_handle;

namespace arki {
class Metadata;

namespace scan {
struct Validator;

namespace grib {
const Validator& validator();
}

class Grib;
struct GribLua;

class Grib : public Scanner
{
protected:
    grib_context* context = nullptr;
    GribLua* L;

public:
    Grib(const std::string& grib1code=std::string(), const std::string& grib2code=std::string());
    virtual ~Grib();

    std::string name() const override { return "grib"; }
    std::unique_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    void scan_singleton(const std::string& abspath, Metadata& md) override;

    friend class GribLua;
};

}
}
#endif
