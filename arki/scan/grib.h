#ifndef ARKI_SCAN_GRIB_H
#define ARKI_SCAN_GRIB_H

/// scan/grib - Scan a GRIB (version 1 or 2) file for metadata

#include <arki/scan.h>
#include <string>
#include <vector>

struct grib_context;
struct grib_handle;

namespace arki {
namespace scan {

namespace grib {
const Validator& validator();
}

struct GribLua;

class GribScanner : public Scanner
{
protected:
    grib_context* context = nullptr;

    void set_source_blob(grib_handle* gh, std::shared_ptr<segment::Reader> reader, FILE* in, Metadata& md);
    void set_source_inline(grib_handle* gh, Metadata& md);

    // Read from gh and add metadata to md
    virtual void scan(grib_handle* gh, Metadata& md) = 0;

public:
    GribScanner();

    std::string name() const override { return "grib"; }
    std::unique_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    void scan_singleton(const std::string& abspath, Metadata& md) override;
};

class Grib : public GribScanner
{
protected:
    GribLua* L;

    void scan(grib_handle* gh, Metadata& md) override;

public:
    Grib(const std::string& grib1code=std::string(), const std::string& grib2code=std::string());
    virtual ~Grib();

    friend class GribLua;
};

}
}
#endif
