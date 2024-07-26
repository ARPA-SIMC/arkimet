#include "odimh5.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/scan/validator.h"
#include "arki/scan/mock.h"
#include <cstring>
#include <unistd.h>
#include <vector>
#include <stdexcept>
#include <string>
#include <sstream>
#include <memory>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace scan {
namespace odimh5 {

/* taken from: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock */
static const unsigned char hdf5sign[8] = { 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a };

struct OdimH5Validator : public Validator
{
    std::string format() const override { return "ODIMH5"; }

    void validate_file(sys::NamedFileDescriptor& fd, off_t offset, size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset, "file segment to check is only " + std::to_string(size) + " bytes (minimum for a ODIMH5 is 8)");

        /* we check that file header is a valid HDF5 header */
        char buf[8];
        ssize_t res;
        if ((res = fd.pread(buf, 8, offset)) != 8)
            throw_check_error(fd, offset, "read only " + std::to_string(res) + "/8 bytes of ODIMH5 header");

        if (memcmp(buf, hdf5sign, 8) != 0)
            throw_check_error(fd, offset, "invalid HDF5 header");
    }

    void validate_buf(const void* buf, size_t size) const override
    {
        /* we check that file header is a valid HDF5 header */

        if (size < 8)
            throw_check_error("buffer is shorter than 8 bytes");

        if (memcmp(buf, hdf5sign, 8) != 0)
            throw_check_error("buffer does not start with hdf5 signature");
    }
};

static OdimH5Validator odimh5_validator;

const Validator& validator() { return odimh5_validator; }

}


/*
 * OdimScanner
 */

void OdimScanner::set_blob_source(Metadata& md, std::shared_ptr<segment::Reader> reader)
{
    struct stat st;
    sys::stat(reader->segment().abspath, st);
    stringstream note;
    note << "Scanned from " << str::basename(reader->segment().relpath);
    md.add_note(note.str());
    md.set_source(Source::createBlob(reader, 0, st.st_size));
}

std::shared_ptr<Metadata> OdimScanner::scan_h5_data(const std::vector<uint8_t>& data)
{
    sys::Tempfile tmpfd;
    tmpfd.write_all_or_throw(data.data(), data.size());
    return scan_h5_file(tmpfd.name());
}

std::shared_ptr<Metadata> OdimScanner::scan_data(const std::vector<uint8_t>& data)
{
    std::shared_ptr<Metadata> md = scan_h5_data(data);
    md->set_source_inline("odimh5", metadata::DataManager::get().to_data("odimh5", std::vector<uint8_t>(data)));
    return md;
}

std::shared_ptr<Metadata> OdimScanner::scan_singleton(const std::filesystem::path& abspath)
{
    return scan_h5_file(abspath);
}

bool OdimScanner::scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    // If the file is empty, skip it
    auto st = sys::stat(reader->segment().abspath);
    if (!st) return true;
    if (S_ISDIR(st->st_mode))
        throw std::runtime_error("OdimH5::scan_segment cannot be called on directory segments");
    if (!st->st_size) return true;

    auto md = scan_h5_file(reader->segment().abspath);
    set_blob_source(*md, reader);
    return dest(md);
}

bool OdimScanner::scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest)
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
 * MockOdimScanner
 */

MockOdimScanner::MockOdimScanner()
{
    engine = new MockEngine();
}

MockOdimScanner::~MockOdimScanner()
{
    delete engine;
}

std::shared_ptr<Metadata> MockOdimScanner::scan_h5_file(const std::filesystem::path& pathname)
{
    auto buf = sys::read_file(pathname);
    return engine->lookup(reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
}

std::shared_ptr<Metadata> MockOdimScanner::scan_h5_data(const std::vector<uint8_t>& data)
{
    return engine->lookup(data.data(), data.size());
}


void register_odimh5_scanner()
{
    Scanner::register_factory("odimh5", [] {
        return std::make_shared<scan::MockOdimScanner>();
    });
}

}
}
