#include "config.h"
#include "arki/scan/nc.h"
#include "arki/file.h"
#include "arki/metadata.h"
#include "arki/runtime/config.h"
#include "arki/utils/files.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/lua.h"
#include "arki/utils/sys.h"
#include "arki/scan/any.h"
#include "arki/types/area.h"
#include "arki/types/time.h"
#include "arki/types/reftime.h"
#include "arki/types/product.h"
#include "arki/types/value.h"
#include <netcdfcpp.h>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <unistd.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace scan {

namespace netcdf {

struct NetCDFValidator : public Validator
{
	virtual ~NetCDFValidator() {}

	std::string format() const override { return "nc"; }

	// Validate data found in a file
	void validate_file(NamedFileDescriptor& fd, off_t offset, size_t size) const override
	{
#if 0
		char buf[1024];

		if (pread(fd, buf, size, offset) == -1)
			throw_system_error(fname);

        std::string s((const char *)buf, size);

		wibble::Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
		if (!re.match(s))
			throw_consistency_error("Not a valid VM2 file", s);
#endif
	}

	// Validate a memory buffer
	void validate_buf(const void* buf, size_t size) const override
	{
#if 0
		std::string s((const char *)buf, size);

		if (size == 0)
			throw_consistency_error("Empty VM2 file");
        wibble::Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
		if (!re.match(s))
			throw_consistency_error("Not a valid VM2 file", s);
#endif
	}
};

static NetCDFValidator nc_validator;

const Validator& validator() { return nc_validator; }

struct Backend
{
    NcFile nc;

    Backend(const std::string& fname)
        : nc(fname.c_str(), NcFile::ReadOnly)
    {
    }
};

}

NetCDF::NetCDF() : backend(0) {}

NetCDF::~NetCDF()
{
    close();
}

void NetCDF::open(const std::string& filename)
{
    string basedir, relname;
    utils::files::resolve_path(filename, basedir, relname);
    open(sys::abspath(filename), basedir, relname);
}

void NetCDF::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
    // Close the previous file if needed
    close();
    this->filename = sys::abspath(filename);
    this->basedir = basedir;
    this->relname = relname;
    if (relname == "-")
        throw std::runtime_error("cannot read NetCDF data from standard input");
    backend = new netcdf::Backend(filename);
}

void NetCDF::close()
{
    if (backend) delete backend;
    backend = 0;
}

bool NetCDF::next(Metadata& md)
{
    if (!backend) return false;

    //NcFile& nc = backend->nc;

#if 0
    meteo::vm2::Value value;
    std::string line;

    off_t offset = 0;
    while (true)
    {
        offset = in->tellg();
        try {
            if (!parser->next(value, line))
                return false;
            else
                break;
        } catch (wibble::exception::Consistency& e) {
            nag::warning("Skipping VM2 line: %s", e.what());
        }
    }

    size_t size = line.size();

    md.create();
    md.source = types::source::Blob::create("vm2", basedir, relname, offset, size);
    md.setCachedData(vector<uint8_t>(line.c_str(), line.size()));
    md.add_note("Scanned from " + relname);
    md.set(types::reftime::Position::create(types::Time::create(value.year, value.month, value.mday, value.hour, value.min, value.sec)));
    md.set(types::area::VM2::create(value.station_id));
    md.set(types::product::VM2::create(value.variable_id));

    // Look for the comma before the value starts
    size_t pos = 0;
    pos = line.find(',', pos);
    pos = line.find(',', pos + 1);
    pos = line.find(',', pos + 1);
    // Store the rest as a value
    md.set(types::Value::create(line.substr(pos+1)));

    return true;
#endif
    return false;
}

}
}
