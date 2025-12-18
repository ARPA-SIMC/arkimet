#include "data.h"
#include "arki/core/file.h"
#include "arki/libconfig.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#ifdef HAVE_GRIBAPI
#include "arki/data/grib.h"
#endif
#ifdef HAVE_DBALLE
#include "arki/data/bufr.h"
#endif
#include "arki/data/jpeg.h"
#include "arki/data/netcdf.h"
#include "arki/data/odimh5.h"
#ifdef HAVE_VM2
#include "arki/data/vm2.h"
#endif
#include <unordered_map>

using namespace std;
using namespace arki::utils;

namespace arki::data {

typedef std::function<std::shared_ptr<Scanner>()> factory;

static std::unordered_map<DataFormat, factory> factories;

static std::unordered_map<DataFormat, std::shared_ptr<Scanner>> scanner_cache;

void init()
{
    factories[DataFormat::GRIB] = [] {
        return std::make_shared<data::MockGribScanner>();
    };
#ifdef HAVE_DBALLE
    factories[DataFormat::BUFR] = [] {
        return std::make_shared<data::MockBufrScanner>();
    };
#endif

    register_odimh5_scanner();
    register_netcdf_scanner();
    register_jpeg_scanner();

#ifdef HAVE_VM2
    factories[DataFormat::VM2] = [] { return std::make_shared<data::Vm2>(); };
#endif
}

/*
 * Scanner
 */

Scanner::~Scanner() {}

void Scanner::register_factory(
    DataFormat name, std::function<std::shared_ptr<Scanner>()> factory)
{
    factories[name] = factory;
    // Clear scanner cache, since from now on the scanners we should create
    // might have changed
    scanner_cache.clear();
}

void Scanner::normalize_before_dispatch(Metadata&) {}

bool Scanner::update_sequence_number(const types::source::Blob&, int&) const
{
    return false;
}

bool Scanner::update_sequence_number(Metadata&, int&) const { return false; }

std::shared_ptr<Scanner> Scanner::get_scanner(DataFormat format)
{
    // Lookup in cache first, before normalisation
    auto cached = scanner_cache.find(format);
    if (cached != scanner_cache.end())
        return cached->second;

    // Instantiate
    auto i = factories.find(format);
    if (i == factories.end())
        throw std::runtime_error("No scanner available for format '" +
                                 format_name(format) + "'");
    auto res              = i->second();
    scanner_cache[format] = res;
    return res;
}

DataFormat Scanner::format_from_filename(const std::filesystem::path& fname)
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

std::optional<DataFormat>
Scanner::detect_format(const std::filesystem::path& path)
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

std::vector<uint8_t> Scanner::reconstruct(DataFormat format, const Metadata& md,
                                          const std::string& value)
{
#ifdef HAVE_VM2
    if (format == DataFormat::VM2)
    {
        return data::Vm2::reconstruct(md, value);
    }
#endif
    throw runtime_error("cannot reconstruct " + format_name(format) +
                        " data: format not supported");
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
    return get(Scanner::format_from_filename(filename));
}

const Validator& Validator::get(DataFormat format)
{
    switch (format)
    {
#ifdef HAVE_GRIBAPI
        case DataFormat::GRIB: return grib::validator();
#endif
#ifdef HAVE_DBALLE
        case DataFormat::BUFR: return bufr::validator();
#endif
#ifdef HAVE_VM2
        case DataFormat::VM2: return vm2::validator();
#endif
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
