#include "defs.h"
#include <string>
#include <ostream>
#include "arki/utils/string.h"

using namespace arki::utils;

namespace arki {

std::string format_names[] = {
    "",
    "grib",
    "bufr",
    "vm2",
    "odimh5",
    "nc",
    "jpeg",
};


const std::string& format_name(DataFormat format)
{
    return format_names[(int)format];
}

DataFormat format_from_string(const std::string& format)
{
    std::string f = str::lower(format);
    if (f == "grib") return DataFormat::GRIB;
    if (f == "grib1") return DataFormat::GRIB;
    if (f == "grib2") return DataFormat::GRIB;

    if (f == "bufr") return DataFormat::BUFR;
    if (f == "vm2") return DataFormat::VM2;

    if (f == "h5")     return DataFormat::ODIMH5;
    if (f == "hdf5")   return DataFormat::ODIMH5;
    if (f == "odim")   return DataFormat::ODIMH5;
    if (f == "odimh5") return DataFormat::ODIMH5;

    if (f == "nc") return DataFormat::NETCDF;
    if (f == "netcdf") return DataFormat::NETCDF;

    if (f == "jpg") return DataFormat::JPEG;
    if (f == "jpeg") return DataFormat::JPEG;

    throw std::runtime_error("unsupported format '" + format + "'");
}

std::ostream& operator<<(std::ostream& o, DataFormat format)
{
    return o << format_name(format);
}

}
