#include "grib.h"
#include <grib_api.h>
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/types/source.h"
#include "arki/exceptions.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/scan/validator.h"
#include "arki/scan/mock.h"
#include "arki/segment.h"
#include <system_error>
#include <cstring>
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

#define check_grib_error(error, ...) do { \
        if (error != GRIB_SUCCESS) { \
            char buf[256]; \
            snprintf(buf, 256, __VA_ARGS__); \
            stringstream ss; \
            ss << buf << ": " << grib_get_error_message(error); \
            throw std::runtime_error(ss.str()); \
        } \
    } while (0)


namespace arki {
namespace scan {

namespace {

struct GribHandle
{
    grib_handle* gh = nullptr;

    GribHandle(grib_context* context, FILE* in)
    {
        int griberror;
        gh = grib_handle_new_from_file(context, in, &griberror);
        if (!gh && griberror == GRIB_END_OF_FILE)
            return;
        check_grib_error(griberror, "reading GRIB from file");
    }
    GribHandle(grib_handle* gh)
        : gh(gh) {}
    GribHandle(const GribHandle&) = delete;
    GribHandle(GribHandle&& o)
        : gh(o.gh)
    {
        o.gh = nullptr;
    }
    GribHandle& operator=(const GribHandle&) = delete;
    GribHandle&& operator=(GribHandle&&) = delete;
    ~GribHandle()
    {
        if (gh)
        {
            grib_handle_delete(gh);
            gh = nullptr;
        }
    }

    void close()
    {
        if (!gh) return;

        check_grib_error(grib_handle_delete(gh), "cannot close GRIB message");
        gh = nullptr;
    }

    operator grib_handle*() { return gh; }

    operator bool() const { return gh != nullptr; }
};

}


GribScanner::GribScanner()
{
    // Get a grib_api context
    context = grib_context_get_default();
    if (!context)
        throw std::runtime_error("cannot get grib_api default context: default context is not available");

    // Multi support is off unless a child class specifies otherwise
    grib_multi_support_off(context);
}

void GribScanner::set_source_blob(grib_handle* gh, std::shared_ptr<segment::Reader> reader, FILE* in, Metadata& md)
{
    // Get the encoded GRIB buffer from the GRIB handle
    const uint8_t* vbuf;
    size_t size;
    check_grib_error(grib_get_message(gh, (const void **)&vbuf, &size), "cannot access the encoded GRIB data");

#if 0
    // We cannot use this as long is too small for 64bit file offsets
    long offset;
    check_grib_error(grib_get_long(gh, "offset", &offset), "reading offset");
#endif
    off_t offset = ftello(in);
    offset -= size;

    md.set_source(Source::createBlob(reader, offset, size));
    md.set_cached_data(metadata::DataManager::get().to_data(reader->segment().format, vector<uint8_t>(vbuf, vbuf + size)));

    stringstream note;
    note << "Scanned from " << str::basename(reader->segment().relpath) << ":" << offset << "+" << size;
    md.add_note(note.str());
}

void GribScanner::set_source_inline(grib_handle* gh, Metadata& md)
{
    // Get the encoded GRIB buffer from the GRIB handle
    const uint8_t* vbuf;
    size_t size;
    check_grib_error(grib_get_message(gh, (const void **)&vbuf, &size), "cannot access the encoded GRIB data");
    md.set_source_inline("grib", metadata::DataManager::get().to_data("grib", vector<uint8_t>(vbuf, vbuf + size)));
}

std::shared_ptr<Metadata> GribScanner::scan_data(const std::vector<uint8_t>& data)
{
    GribHandle gh(grib_handle_new_from_message(context, (void*)data.data(), data.size()));
    if (!gh) throw std::runtime_error("GRIB memory buffer failed to scan");

    std::shared_ptr<Metadata> md = scan(gh);
    md->set_source_inline("grib", metadata::DataManager::get().to_data("grib", std::vector<uint8_t>(data)));


    gh.close();

    return md;
}

bool GribScanner::scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    files::RAIIFILE in(reader->segment().abspath, "rb");
    while (true)
    {
        GribHandle gh(context, in);
        if (!gh) break;
        std::shared_ptr<Metadata> md = scan(gh);
        set_source_blob(gh, reader, in, *md);
        gh.close();

        if (!dest(md)) return false;
    }
    return true;
}

std::shared_ptr<Metadata> GribScanner::scan_singleton(const std::string& abspath)
{
    std::shared_ptr<Metadata> md;
    files::RAIIFILE in(abspath, "rb");
    {
        GribHandle gh(context, in);
        if (!gh) throw std::runtime_error(abspath + " contains no GRIB data");
        md = scan(gh);
        stringstream note;
        note << "Scanned from " << str::basename(abspath);
        md->add_note(note.str());
        gh.close();
    }

    {
        GribHandle gh(context, in);
        if (gh) throw std::runtime_error(abspath + " contains more than one GRIB data");
        gh.close();
    }

    return md;
}

bool GribScanner::scan_pipe(core::NamedFileDescriptor& infd, metadata_dest_func dest)
{
    files::RAIIFILE in(infd, "rb");
    while (true)
    {
        GribHandle gh(context, in);
        if (!gh) break;
        std::shared_ptr<Metadata> md = scan(gh);
        set_source_inline(gh, *md);
        stringstream note;
        note << "Scanned from standard input";
        md->add_note(note.str());

        if (!dest(md)) return false;
    }
    return true;
}

MockGribScanner::MockGribScanner()
{
    engine = new MockEngine();
}

MockGribScanner::~MockGribScanner()
{
    delete engine;
}

std::shared_ptr<Metadata> MockGribScanner::scan(grib_handle* gh)
{
    // Get the encoded GRIB buffer from the GRIB handle
    const uint8_t* vbuf;
    size_t size;
    check_grib_error(grib_get_message(gh, (const void **)&vbuf, &size), "cannot access the encoded GRIB data");

    return engine->lookup(vbuf, size);
}

namespace grib {

struct GribValidator : public Validator
{
    std::string format() const override { return "GRIB"; }

    // Validate data found in a file
    void validate_file(sys::NamedFileDescriptor& fd, off_t offset, size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset, "file segment to check is only " + std::to_string(size) + " bytes (minimum for a GRIB is 8)");

        char buf[4];
        ssize_t res = fd.pread(buf, 4, offset);
        if (res != 4)
            throw_check_error(fd, offset, "read only " + std::to_string(res) + "/4 bytes of GRIB header");
        if (memcmp(buf, "GRIB", 4) != 0)
            throw_check_error(fd, offset, "data does not start with 'GRIB'");
        res = fd.pread(buf, 4, offset + size - 4);
        if (res != 4)
            throw_check_error(fd, offset, "read only " + std::to_string(res) + "/4 bytes of GRIB trailer");
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

}

}
}
