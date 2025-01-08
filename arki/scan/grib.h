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
class MockEngine;

namespace grib {
const Validator& validator();
}

class GribScanner : public Scanner
{
protected:
    grib_context* context = nullptr;

    void set_source_blob(grib_handle* gh, std::shared_ptr<segment::data::Reader> reader, FILE* in, Metadata& md);
    void set_source_inline(grib_handle* gh, Metadata& md);

    // Read from gh and add metadata to md
    virtual std::shared_ptr<Metadata> scan(grib_handle* gh) = 0;

public:
    GribScanner();

    std::string name() const override { return "grib"; }
    std::shared_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_segment(std::shared_ptr<segment::data::Reader> reader, metadata_dest_func dest) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    std::shared_ptr<Metadata> scan_singleton(const std::filesystem::path& abspath) override;
};

class MockGribScanner : public GribScanner
{
protected:
    MockEngine* engine;

    std::shared_ptr<Metadata> scan(grib_handle* gh) override;

public:
    MockGribScanner();
    virtual ~MockGribScanner();
};

}
}
#endif
