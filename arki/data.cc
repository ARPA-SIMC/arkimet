#include "data.h"
#include "core/file.h"
#include "data/bufr.h"
#include "data/grib.h"
#include "data/jpeg.h"
#include "data/mock.h"
#include "data/netcdf.h"
#include "data/odimh5.h"
#include "data/vm2.h"
#include "metadata.h"
#include "metadata/data.h"
#include "segment.h"
#include "types/source.h"
#include "types/source/blob.h"
#include "utils/files.h"
#include "utils/string.h"
#include "utils/sys.h"
#include <sys/mman.h>
#include <unordered_map>

using namespace std;
using namespace arki::utils;

namespace arki::data {

typedef std::function<std::shared_ptr<Scanner>()> factory;

static std::vector<factory>
    factories(static_cast<unsigned>(DataFormat::__END__));
static std::vector<std::shared_ptr<Scanner>>
    scanner_cache(static_cast<unsigned>(DataFormat::__END__));

void init()
{
    // Initialize a mock scanner for all formats
    for (unsigned format = static_cast<unsigned>(DataFormat::GRIB);
         format < static_cast<unsigned>(DataFormat::__END__); ++format)
        factories[format] = [=] {
            return std::make_shared<data::MockScanner>(
                static_cast<DataFormat>(format));
        };

    // Install the scanners for known formats that need special handling
    factories[(unsigned)DataFormat::GRIB] = [] {
        return std::make_shared<data::grib::MockScanner>();
    };
    factories[(unsigned)DataFormat::BUFR] = [] {
        return std::make_shared<data::bufr::MockScanner>();
    };
    factories[(unsigned)DataFormat::VM2] = [] {
        return std::make_shared<data::vm2::Scanner>();
    };
}

DataFormat format_from_filename(const std::filesystem::path& fname)
{
    // Extract the extension
    auto ext = fname.extension();
    if (not ext.empty())
    {
        if (ext == ".zip" || ext == ".gz" || ext == ".tar")
            ext = fname.stem().extension();
    }

    if (not ext.empty())
    {
        std::string f = str::lower(ext);

        if (f == ".grib")
            return DataFormat::GRIB;
        if (f == ".grib1")
            return DataFormat::GRIB;
        if (f == ".grib2")
            return DataFormat::GRIB;

        if (f == ".bufr")
            return DataFormat::BUFR;
        if (f == ".vm2")
            return DataFormat::VM2;

        if (f == ".h5")
            return DataFormat::ODIMH5;
        if (f == ".hdf5")
            return DataFormat::ODIMH5;
        if (f == ".odim")
            return DataFormat::ODIMH5;
        if (f == ".odimh5")
            return DataFormat::ODIMH5;

        if (f == ".nc")
            return DataFormat::NETCDF;
        if (f == ".netcdf")
            return DataFormat::NETCDF;

        if (f == ".jpg")
            return DataFormat::JPEG;
        if (f == ".jpeg")
            return DataFormat::JPEG;

        throw std::runtime_error("unsupported extension '" + f + "'");
    }
    else
    {
        stringstream ss;
        ss << "cannot auto-detect format from file name " << fname
           << ": file extension not recognised";
        throw std::runtime_error(ss.str());
    }
}

std::optional<DataFormat> detect_format(const std::filesystem::path& path)
{
    // Extract the extension
    auto ext = path.extension();
    if (not ext.empty())
    {
        if (ext == ".zip" || ext == ".gz" || ext == ".tar")
            ext = path.stem().extension();
    }

    if (ext.empty())
        return std::optional<DataFormat>();

    std::string f = str::lower(ext);

    if (f == ".grib")
        return DataFormat::GRIB;
    if (f == ".grib1")
        return DataFormat::GRIB;
    if (f == ".grib2")
        return DataFormat::GRIB;

    if (f == ".bufr")
        return DataFormat::BUFR;
    if (f == ".vm2")
        return DataFormat::VM2;

    if (f == ".h5")
        return DataFormat::ODIMH5;
    if (f == ".hdf5")
        return DataFormat::ODIMH5;
    if (f == ".odim")
        return DataFormat::ODIMH5;
    if (f == ".odimh5")
        return DataFormat::ODIMH5;

    if (f == ".nc")
        return DataFormat::NETCDF;
    if (f == ".netcdf")
        return DataFormat::NETCDF;

    if (f == ".jpg")
        return DataFormat::JPEG;
    if (f == ".jpeg")
        return DataFormat::JPEG;

    return std::optional<DataFormat>();
}

/*
 * Scanner
 */

Scanner::~Scanner() {}

void Scanner::register_factory(
    DataFormat name, std::function<std::shared_ptr<Scanner>()> factory)
{
    unsigned idx       = static_cast<unsigned>(name);
    factories[idx]     = factory;
    scanner_cache[idx] = std::shared_ptr<arki::data::Scanner>();
}

void Scanner::normalize_before_dispatch(Metadata&) {}

bool Scanner::update_sequence_number(const types::source::Blob&, int&) const
{
    return false;
}

bool Scanner::update_sequence_number(Metadata&, int&) const { return false; }

std::shared_ptr<Scanner> Scanner::get(DataFormat format)
{
    unsigned idx = static_cast<unsigned>(format);
    if (idx >= static_cast<unsigned>(DataFormat::__END__))
        throw std::runtime_error("No scanner available for format '" +
                                 format_name(format) + "'");

    // Lookup in cache first, before normalisation
    if (auto cached = scanner_cache[idx])
        return cached;

    // Instantiate
    auto res = factories[idx]();
    if (!res)
        throw std::runtime_error("arki::data::init() has not been called");
    scanner_cache[idx] = res;
    return res;
}

std::vector<uint8_t> Scanner::reconstruct(const Metadata&,
                                          const std::string&) const
{
    throw runtime_error("cannot reconstruct " + format_name(name()) +
                        " data: format not supported");
}

/*
 * SingleFileScanner
 */

bool SingleFileScanner::scan_segment(std::shared_ptr<segment::Reader> reader,
                                     metadata_dest_func dest)
{
    // If the file is empty, skip it
    auto st = sys::stat(reader->segment().abspath());
    if (!st)
        return true;
    if (S_ISDIR(st->st_mode))
        throw std::runtime_error(
            format_name(name()) +
            ": scan_segment cannot be called on directory segments");
    if (!st->st_size)
        return true;

    auto md = scan_file_single(reader->segment().abspath());

    md->add_note_scanned_from(reader->segment().relpath());
    md->set_source(types::Source::createBlob(reader, 0, st->st_size));

    sys::File fd(reader->segment().abspath(), O_RDONLY);
    sys::MMap mapped_data = fd.mmap(st->st_size, PROT_READ, MAP_PRIVATE);
    std::vector<uint8_t> data(static_cast<const uint8_t*>(mapped_data),
                              static_cast<const uint8_t*>(mapped_data) +
                                  mapped_data.size());
    md->set_cached_data(metadata::DataManager::get().to_data(
        reader->segment().format(), std::move(data)));

    return dest(md);
}

/*
 * NullValidator
 */

/// Validator that considers everything always valid
class NullValidator : public Validator
{
    DataFormat data_format;

public:
    explicit NullValidator(DataFormat format) : data_format(format) {}

    DataFormat format() const override { return data_format; }

    // Validate data found in a file
    void validate_file(core::NamedFileDescriptor&, off_t, size_t) const override
    {
    }

    // Validate a memory buffer
    virtual void validate_buf(const void*, size_t) const override {}

    // Validate a metadata::Data
    virtual void validate_data(const metadata::Data&) const override {}
};

std::vector<NullValidator*> null_validators;

/*
 * Validator
 */

void Validator::throw_check_error(utils::sys::NamedFileDescriptor& fd,
                                  off_t offset, const std::string& msg) const
{
    throw_runtime_error(fd.path(), ":", offset, ": ", format(),
                        " validation failed: ", msg);
}

void Validator::throw_check_error(const std::string& msg) const
{
    throw_runtime_error(format(), " validation failed: ", msg);
}

const Validator& Validator::by_filename(const std::filesystem::path& filename)
{
    return get(format_from_filename(filename));
}

const Validator& Validator::get(DataFormat format)
{
    switch (format)
    {
        case DataFormat::GRIB:   return grib::validator();
        case DataFormat::BUFR:   return bufr::validator();
        case DataFormat::VM2:    return vm2::validator();
        case DataFormat::ODIMH5: return odimh5::validator();
        case DataFormat::NETCDF: return netcdf::validator();
        case DataFormat::JPEG:   return jpeg::validator();
        default:                 {
            unsigned idx = (unsigned)format;
            if (null_validators.size() <= idx + 1)
                null_validators.resize(idx + 1);
            if (null_validators[idx] == nullptr)
                null_validators[idx] = new NullValidator(format);
            return *null_validators[idx];
        }
    }
}

void Validator::validate_data(const metadata::Data& data) const
{
    auto buf = data.read();
    validate_buf(buf.data(), buf.size());
}

} // namespace arki::data
