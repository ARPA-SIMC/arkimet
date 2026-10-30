#include "bufr.h"
#include "arki/libconfig.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/types/timerange.h"
#include "arki/types/values.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "mock.h"
#include <cstring>
#ifdef HAVE_DBALLE
#include <dballe/file.h>
#include <dballe/importer.h>
#include <dballe/message.h>
#include <wreport/bulletin.h>
#include <wreport/options.h>
#endif
#include <sstream>

using namespace std;
using namespace wreport;
using namespace dballe;
using namespace arki::types;
using namespace arki::utils;

namespace arki::data::bufr {

struct BufrValidator : public Validator
{
    DataFormat format() const override { return DataFormat::BUFR; }

    // Validate data found in a file
    void validate_file(sys::NamedFileDescriptor& fd, off_t offset,
                       size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset,
                              "file segment to check is only " +
                                  std::to_string(size) +
                                  " bytes (minimum for a BUFR is 8)");

        char buf[4];
        ssize_t res;
        if ((res = fd.pread(buf, 4, offset)) != 4)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/4 bytes of BUFR header");
        if (memcmp(buf, "BUFR", 4) != 0)
            throw_check_error(fd, offset, "data does not start with 'BUFR'");
        if ((res = fd.pread(buf, 4, offset + size - 4)) != 4)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/4 bytes of BUFR trailer");
        if (memcmp(buf, "7777", 4) != 0)
            throw_check_error(fd, offset, "data does not end with '7777'");
    }

    // Validate a memory buffer
    void validate_buf(const void* buf, size_t size) const override
    {
        if (size < 8)
            throw_check_error("buffer is shorter than 8 bytes");
        if (memcmp(buf, "BUFR", 4) != 0)
            throw_check_error("buffer does not start with 'BUFR'");
        if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
            throw_check_error("buffer does not end with '7777'");
    }
};

static BufrValidator bufr_validator;

const Validator& validator() { return bufr_validator; }

/*
 * MockScanner
 */

#ifdef HAVE_DBALLE

void MockScanner::do_scan(dballe::BinaryMessage& rmsg,
                          std::shared_ptr<Metadata> md)
{
    auto new_md = MockEngine::get().lookup(
        reinterpret_cast<const uint8_t*>(rmsg.data.data()), rmsg.data.size());
    md->merge(*new_md);
}

bool MockScanner::scan_file(const std::filesystem::path& abspath,
                            metadata_dest_func dest)
{
    auto file =
        dballe::File::create(dballe::Encoding::BUFR, abspath.c_str(), "r");
    while (true)
    {
        auto md            = std::make_shared<Metadata>();
        BinaryMessage rmsg = file->read();
        if (!rmsg)
            break;
        std::vector<uint8_t> data(rmsg.data.begin(), rmsg.data.end());
        md->set_source(arki::types::Source::createBlobUnlocked(
            DataFormat::BUFR, abspath.parent_path(), abspath.filename(),
            rmsg.offset, data.size()));
        md->set_cached_data(metadata::DataManager::get().to_data(
            DataFormat::BUFR, std::move(data)));

        do_scan(rmsg, md);

        if (!dest(md))
            return false;
    }
    return true;
}

bool MockScanner::scan_pipe(core::NamedFileDescriptor& infd,
                            metadata_dest_func dest)
{
    files::RAIIFILE in(infd, "rb");
    auto file =
        dballe::File::create(dballe::Encoding::BUFR, in, false, infd.path());
    while (true)
    {
        auto md            = std::make_shared<Metadata>();
        BinaryMessage rmsg = file->read();
        if (!rmsg)
            break;
        md->set_source_inline(
            DataFormat::BUFR,
            metadata::DataManager::get().to_data(
                DataFormat::BUFR,
                vector<uint8_t>(rmsg.data.begin(), rmsg.data.end())));
        do_scan(rmsg, md);
        if (!dest(std::move(md)))
            return false;
    }
    return true;
}

std::shared_ptr<Metadata>
MockScanner::scan_data(const std::vector<uint8_t>& data)
{
    auto md = std::make_shared<Metadata>();
    md->set_source_inline(DataFormat::BUFR,
                          metadata::DataManager::get().to_data(
                              DataFormat::BUFR, std::vector<uint8_t>(data)));
    BinaryMessage rmsg(Encoding::BUFR);
    rmsg.data = std::string(data.begin(), data.end());
    do_scan(rmsg, md);
    return md;
}

bool MockScanner::update_sequence_number(std::shared_ptr<Metadata> md,
                                         int& usn) const
{
    // Update Sequence Numbers are only supported by BUFR
    if (md->source().format != DataFormat::BUFR)
        return false;

    const auto& data = md->get_data();
    auto buf         = data.read();
    std::string strbuf((const char*)buf.data(), buf.size());
    auto bulletin = BufrBulletin::decode_header(strbuf);
    usn           = bulletin->update_sequence_number;
    return true;
}

#else

void MockScanner::do_scan(dballe::BinaryMessage& rmsg,
                          std::shared_ptr<Metadata> md)
{
    throw std::runtime_error("BUFR support is not available");
}

bool MockScanner::scan_file(const std::filesystem::path& abspath,
                            metadata_dest_func dest)
{
    throw std::runtime_error("BUFR support is not available");
}

bool MockScanner::scan_pipe(core::NamedFileDescriptor& in,
                            metadata_dest_func dest)
{
    throw std::runtime_error("BUFR support is not available");
}

std::shared_ptr<Metadata>
MockScanner::scan_data(const std::vector<uint8_t>& data)
{
    throw std::runtime_error("BUFR support is not available");
}

bool MockScanner::update_sequence_number(std::shared_ptr<Metadata> md,
                                         int& usn) const
{
    throw std::runtime_error("BUFR support is not available");
}
#endif

} // namespace arki::data::bufr
