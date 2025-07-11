#include "jpeg.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/scan/mock.h"
#include "arki/scan/validator.h"
#include "arki/segment/data.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <cstring>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace scan {
namespace jpeg {

struct JPEGValidator : public Validator
{
    // For reference about signatures, see
    // https://en.wikipedia.org/wiki/JPEG#Syntax_and_structure

    DataFormat format() const override { return DataFormat::JPEG; }

    void validate_file(sys::NamedFileDescriptor& fd, off_t offset,
                       size_t size) const override
    {
        if (size < 4)
            throw_check_error(
                fd, offset,
                "file segment to check is only " + std::to_string(size) +
                    " bytes (minimum required for JPEG identification is 4)");

        // check that the file begins and ends with the right markers
        unsigned char buf[2];
        ssize_t res;
        if ((res = fd.pread(buf, 2, offset)) != 2)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/2 bytes of JPEG header");

        if (buf[0] != 0xff || buf[1] != 0xd8)
            throw_check_error(fd, offset,
                              "JPEG Start Of Image signature not found");

        if ((res = fd.pread(buf, 2, offset + size - 2)) != 2)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/2 bytes of JPEG trailer");

        if (buf[0] != 0xff || buf[1] != 0xd9)
            throw_check_error(fd, offset,
                              "JPEG End Of Image signature not found");
    }

    void validate_buf(const void* buf, size_t size) const override
    {
        /* we check that file header is a valid HDF5 header */

        if (size < 4)
            throw_check_error("buffer is shorter than 4 bytes");

        const unsigned char* chunk =
            reinterpret_cast<const unsigned char*>(buf);
        if (chunk[0] != 0xff || chunk[1] != 0xd8)
            throw_check_error("JPEG Start Of Image signature not found");

        chunk += size - 2;
        if (chunk[0] != 0xff || chunk[1] != 0xd9)
            throw_check_error("JPEG End Of Image signature not found");
    }
};

static JPEGValidator jpeg_validator;

const Validator& validator() { return jpeg_validator; }

} // namespace jpeg

/*
 * JPEGScanner
 */

void JPEGScanner::set_blob_source(Metadata& md,
                                  std::shared_ptr<segment::data::Reader> reader)
{
    struct stat st;
    sys::stat(reader->segment().abspath(), st);
    md.add_note_scanned_from(reader->segment().relpath());
    md.set_source(Source::createBlob(reader, 0, st.st_size));
}

std::shared_ptr<Metadata>
JPEGScanner::scan_data(const std::vector<uint8_t>& data)
{
    std::shared_ptr<Metadata> md = scan_jpeg_data(data);
    md->set_source_inline(DataFormat::JPEG,
                          metadata::DataManager::get().to_data(
                              DataFormat::JPEG, std::vector<uint8_t>(data)));
    return md;
}

std::shared_ptr<Metadata>
JPEGScanner::scan_singleton(const std::filesystem::path& abspath)
{
    return scan_jpeg_file(abspath);
}

bool JPEGScanner::scan_segment(std::shared_ptr<segment::data::Reader> reader,
                               metadata_dest_func dest)
{
    // If the file is empty, skip it
    auto st = sys::stat(reader->segment().abspath());
    if (!st)
        return true;
    if (S_ISDIR(st->st_mode))
        throw std::runtime_error(
            "JPEGScanner::scan_segment cannot be called on directory segments");
    if (!st->st_size)
        return true;

    auto md = scan_jpeg_file(reader->segment().abspath());
    set_blob_source(*md, reader);
    return dest(md);
}

bool JPEGScanner::scan_pipe(core::NamedFileDescriptor& in,
                            metadata_dest_func dest)
{
    // Read all in a buffer
    std::vector<uint8_t> buf;
    const unsigned blocksize = 4096;
    while (true)
    {
        buf.resize(buf.size() + blocksize);
        unsigned read = in.read(buf.data() + buf.size() - blocksize, blocksize);
        if (read < blocksize)
        {
            buf.resize(buf.size() - blocksize + read);
            break;
        }
    }

    return dest(scan_data(buf));
}

/*
 * MockJPEGScanner
 */

MockJPEGScanner::MockJPEGScanner() { engine = new MockEngine(); }

MockJPEGScanner::~MockJPEGScanner() { delete engine; }

std::shared_ptr<Metadata>
MockJPEGScanner::scan_jpeg_file(const std::filesystem::path& pathname)
{
    auto buf = sys::read_file(pathname);
    return engine->lookup(reinterpret_cast<const uint8_t*>(buf.data()),
                          buf.size());
}

std::shared_ptr<Metadata>
MockJPEGScanner::scan_jpeg_data(const std::vector<uint8_t>& data)
{
    return engine->lookup(data.data(), data.size());
}

void register_jpeg_scanner()
{
    Scanner::register_factory(DataFormat::JPEG, [] {
        return std::make_shared<scan::MockJPEGScanner>();
    });
}

} // namespace scan
} // namespace arki
