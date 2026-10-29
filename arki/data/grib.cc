#include "grib.h"
#include "arki/libconfig.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment.h"
#include "arki/types/source.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "mock.h"
#include <cstring>
#ifdef HAVE_GRIBAPI
#include <grib_api.h>
#endif
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

#define check_grib_error(error, ...)                                           \
    do                                                                         \
    {                                                                          \
        if (error != GRIB_SUCCESS)                                             \
        {                                                                      \
            char buf[256];                                                     \
            snprintf(buf, 256, __VA_ARGS__);                                   \
            stringstream ss;                                                   \
            ss << buf << ": " << grib_get_error_message(error);                \
            throw std::runtime_error(ss.str());                                \
        }                                                                      \
    } while (0)

namespace arki::data::grib {

#ifdef HAVE_GRIBAPI

/*
 * Scan
 */

Scan::Scan()
{
    // Get a grib_api context
    context = grib_context_get_default();
    if (!context)
        throw std::runtime_error("cannot get grib_api default context: default "
                                 "context is not available");

    // Multi support is off unless a child class specifies otherwise
    grib_multi_support_off(context);
}

Scan::~Scan()
{
    if (gh)
    {
        grib_handle_delete(gh);
        gh = nullptr;
    }
}

void Scan::close_handle()
{
    if (!gh)
        return;

    check_grib_error(grib_handle_delete(gh), "cannot close GRIB message");
    gh = nullptr;
}

grib_handle* Scan::handle() const { return gh; }

std::vector<uint8_t> Scan::grib_data() const
{
    const uint8_t* vbuf;
    size_t size;
    check_grib_error(grib_get_message(gh, (const void**)&vbuf, &size),
                     "cannot access the encoded GRIB data");
    return std::vector<uint8_t>(vbuf, vbuf + size);
}

/*
 * ScanFile
 */

ScanFile::ScanFile(const std::filesystem::path& path)
    : Scan(), file(path, "rb"), path(path)
{
}

bool ScanFile::next()
{
    close_handle();
    int griberror;
    gh = grib_handle_new_from_file(context, file, &griberror);
    if (!gh)
    {
        if (griberror != GRIB_END_OF_FILE)
            check_grib_error(griberror, "cannot read GRIB from %s",
                             path.c_str());
        return false;
    }
    return true;
}

std::shared_ptr<Metadata> ScanFile::metadata() const
{
    // Create and return a metadata with the right source information
    auto md   = std::make_shared<Metadata>();
    auto data = grib_data();

#if 0
    // We cannot use this as long is too small for 64bit file offsets
    long offset;
    check_grib_error(grib_get_long(gh, "offset", &offset), "reading offset");
#endif
    off_t offset = ftello(file.fd);
    offset -= data.size();

    md->set_source(arki::types::Source::createBlobUnlocked(
        DataFormat::GRIB, path.parent_path(), path.filename(), offset,
        data.size()));
    md->set_cached_data(metadata::DataManager::get().to_data(DataFormat::GRIB,
                                                             std::move(data)));

    return md;
}

/*
 * ScanPipe
 */

ScanPipe::ScanPipe(sys::NamedFileDescriptor& ifd) : file(ifd, "rb") {}

bool ScanPipe::next()
{
    close_handle();
    int griberror;
    gh = grib_handle_new_from_file(context, file, &griberror);
    if (!gh)
    {
        if (griberror != GRIB_END_OF_FILE)
            check_grib_error(griberror, "cannot read GRIB from pipe");
        return false;
    }
    return true;
}

std::shared_ptr<Metadata> ScanPipe::metadata() const
{
    // Create and return a metadata with the right source information
    auto md = std::make_shared<Metadata>();
    md->set_source_inline(
        DataFormat::GRIB,
        metadata::DataManager::get().to_data(DataFormat::GRIB, grib_data()));
    return md;
}

/*
 * ScanData
 */

ScanData::ScanData(const std::vector<uint8_t>& data) : data(data) {}

bool ScanData::next()
{
    // Only generate one GRIB
    bool eof = gh != nullptr;
    close_handle();
    if (eof)
        return false;

    gh = grib_handle_new_from_message(context, data.data(), data.size());
    if (!gh)
        throw std::runtime_error("cannot read GRIB from memory");
    return true;
}

std::shared_ptr<Metadata> ScanData::metadata() const
{
    // Create and return a metadata with the right source information
    auto md = std::make_shared<Metadata>();
    md->set_source_inline(
        DataFormat::GRIB,
        metadata::DataManager::get().to_data(DataFormat::GRIB, grib_data()));
    return md;
}

/*
 * MockScanner
 */
void MockScanner::scan(grib_handle* gh, std::shared_ptr<Metadata> md)
{
    // Get the encoded GRIB buffer from the GRIB handle
    const uint8_t* vbuf;
    size_t size;
    check_grib_error(grib_get_message(gh, (const void**)&vbuf, &size),
                     "cannot access the encoded GRIB data");
    auto new_md = MockEngine::get().lookup(vbuf, size);
    md->merge(*new_md);
}

bool MockScanner::scan_file(const std::filesystem::path& abspath,
                            metadata_dest_func dest)
{
    ScanFile gribs(abspath);
    while (gribs.next())
    {
        auto md = gribs.metadata();
        scan(gribs.handle(), md);
        if (!dest(md))
            return false;
    }
    return true;
}

bool MockScanner::scan_pipe(core::NamedFileDescriptor& in,
                            metadata_dest_func dest)
{
    ScanPipe gribs(in);
    while (gribs.next())
    {
        auto md = gribs.metadata();
        scan(gribs.handle(), md);
        if (!dest(md))
            return false;
    }
    return true;
}

std::shared_ptr<Metadata>
MockScanner::scan_data(const std::vector<uint8_t>& data)
{
    ScanData gribs(data);
    std::shared_ptr<Metadata> md;
    while (gribs.next())
    {
        md = gribs.metadata();
        scan(gribs.handle(), md);
    }
    return md;
}

#else

Scan::Scan() { throw std::runtime_error("GRIB support is not available"); }

Scan::~Scan() {}

void Scan::close_handle() {}

grib_handle* Scan::handle() const
{
    throw std::runtime_error("GRIB support is not available");
}

std::vector<uint8_t> Scan::grib_data() const
{
    throw std::runtime_error("GRIB support is not available");
}

ScanFile::ScanFile(const std::filesystem::path& path)
    : Scan(), file(path, "rb"), path(path)
{
}

bool ScanFile::next()
{
    throw std::runtime_error("GRIB support is not available");
}

std::shared_ptr<Metadata> ScanFile::metadata() const
{
    throw std::runtime_error("GRIB support is not available");
}

ScanPipe::ScanPipe(sys::NamedFileDescriptor& ifd) : file(ifd, "rb") {}

bool ScanPipe::next()
{
    throw std::runtime_error("GRIB support is not available");
}

std::shared_ptr<Metadata> ScanPipe::metadata() const
{
    throw std::runtime_error("GRIB support is not available");
}

ScanData::ScanData(const std::vector<uint8_t>& data) : data(data) {}

bool ScanData::next()
{
    throw std::runtime_error("GRIB support is not available");
}

std::shared_ptr<Metadata> ScanPipe::metadata() const
{
    throw std::runtime_error("GRIB support is not available");
}

std::shared_ptr<Metadata> MockScanner::scan(grib_handle* gh)
{
    throw std::runtime_error("GRIB support is not available");
}
#endif

struct GribValidator : public Validator
{
    DataFormat format() const override { return DataFormat::GRIB; }

    // Validate data found in a file
    void validate_file(sys::NamedFileDescriptor& fd, off_t offset,
                       size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset,
                              "file segment to check is only " +
                                  std::to_string(size) +
                                  " bytes (minimum for a GRIB is 8)");

        char buf[4];
        ssize_t res = fd.pread(buf, 4, offset);
        if (res != 4)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/4 bytes of GRIB header");
        if (memcmp(buf, "GRIB", 4) != 0)
            throw_check_error(fd, offset, "data does not start with 'GRIB'");
        res = fd.pread(buf, 4, offset + size - 4);
        if (res != 4)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/4 bytes of GRIB trailer");
        if (memcmp(buf, "7777", 4) != 0)
            throw_check_error(fd, offset, "data does not end with '7777'");
    }

    // Validate a memory buffer
    void validate_buf(const void* buf, size_t size) const override
    {
        if (size < 8)
            throw_check_error("buffer is shorter than 8 bytes");
        if (memcmp(buf, "GRIB", 4) != 0)
            throw_check_error("buffer does not start with 'GRIB'");
        if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
            throw_check_error("buffer does not end with '7777'");
    }
};

static GribValidator grib_validator;

const Validator& validator() { return grib_validator; }

} // namespace arki::data::grib
