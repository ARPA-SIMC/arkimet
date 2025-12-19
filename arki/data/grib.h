#ifndef ARKI_DATA_GRIB_H
#define ARKI_DATA_GRIB_H

/// scan/grib - Scan a GRIB (version 1 or 2) file for metadata

#include <arki/data.h>
#include <string>
#include <vector>

struct grib_context;
struct grib_handle;

namespace arki::data {
class MockEngine;

namespace grib {
const Validator& validator();

/// RAII wrapper for an eccodes GRIB handle
struct GribHandle
{
    grib_handle* gh = nullptr;

    GribHandle(grib_context* context, FILE* in);
    explicit GribHandle(grib_handle* gh) : gh(gh) {}
    GribHandle(const GribHandle&) = delete;
    GribHandle(GribHandle&& o) : gh(o.gh) { o.gh = nullptr; }
    GribHandle& operator=(const GribHandle&) = delete;
    GribHandle&& operator=(GribHandle&&)     = delete;
    ~GribHandle();

    void close();

    operator grib_handle*() { return gh; }

    operator bool() const { return gh != nullptr; }

    grib_handle* release()
    {
        auto res = gh;
        gh       = nullptr;
        return res;
    }
};

} // namespace grib

class GribScanner : public Scanner
{
protected:
    grib_context* context = nullptr;

    void set_source_blob(grib_handle* gh,
                         std::shared_ptr<segment::Reader> reader, FILE* in,
                         Metadata& md);
    void set_source_inline(grib_handle* gh, Metadata& md);

    // Read from gh and add metadata to md
    virtual std::shared_ptr<Metadata> scan(grib_handle* gh) = 0;

public:
    GribScanner();

    DataFormat name() const override { return DataFormat::GRIB; }
    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader,
                      metadata_dest_func dest) override;
    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;
    std::shared_ptr<Metadata>
    scan_singleton(const std::filesystem::path& abspath) override;
};

class MockGribScanner : public GribScanner
{
protected:
    std::shared_ptr<Metadata> scan(grib_handle* gh) override;
};

} // namespace arki::data
#endif
