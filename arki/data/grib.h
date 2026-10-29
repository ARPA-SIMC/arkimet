#ifndef ARKI_DATA_GRIB_H
#define ARKI_DATA_GRIB_H

/// scan/grib - Scan a GRIB (version 1 or 2) file for metadata

#include <arki/data.h>
#include <arki/utils/files.h>
#include <string>
#include <vector>

struct grib_context;
struct grib_handle;

namespace arki::data::grib {

const Validator& validator();

/// Iterate GRIB messages for scanning
struct Scan
{
protected:
    grib_context* context = nullptr;
    grib_handle* gh       = nullptr;

    /// Close the current handle and set it to nullptr
    void close_handle();

    /// Get the encoded GRIB buffer from the GRIB handle
    std::vector<uint8_t> grib_data() const;

public:
    Scan();
    virtual ~Scan();

    /// Move to the next message
    virtual bool next() = 0;
    /// Return the current GRIB handle
    grib_handle* handle() const;
    /// Return the metadata for the current GRIB handle
    virtual std::shared_ptr<Metadata> metadata() const = 0;
};

/// Iterate GRIB messages from a file
struct ScanFile : public Scan
{
protected:
    arki::utils::files::RAIIFILE file;
    std::filesystem::path path;

public:
    explicit ScanFile(const std::filesystem::path& path);
    bool next() override;
    std::shared_ptr<Metadata> metadata() const override;
};

/// Iterate GRIB messages from a pipe
struct ScanPipe : public Scan
{
protected:
    arki::utils::files::RAIIFILE file;

public:
    explicit ScanPipe(arki::utils::sys::NamedFileDescriptor& ifd);
    bool next() override;
    std::shared_ptr<Metadata> metadata() const override;
};

/// Iterate GRIB messages from a data buffer
struct ScanData : public Scan
{
protected:
    std::vector<uint8_t> data;

public:
    explicit ScanData(const std::vector<uint8_t>& data);
    bool next() override;
    std::shared_ptr<Metadata> metadata() const override;
};

/**
 * Scan GRIB data using the mock scan engine used for tests
 */
class MockScanner : public data::Scanner
{
protected:
    void scan(grib_handle* gh, std::shared_ptr<Metadata>);

public:
    DataFormat name() const override { return DataFormat::GRIB; }

    bool scan_file(const std::filesystem::path& abspath,
                   metadata_dest_func dest) override;

    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;

    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;
};

} // namespace arki::data::grib
#endif
