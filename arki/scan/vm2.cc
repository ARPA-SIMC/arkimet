#include <arki/scan/vm2.h>
#include <arki/types/source/blob.h>
#include <arki/metadata.h>
#include <arki/runtime/config.h>
#include <arki/utils/files.h>
#include <arki/nag.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <wibble/regexp.h>
#include <arki/utils/lua.h>
#include <arki/scan/any.h>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <unistd.h>

#include <arki/types/area.h>
#include <arki/types/time.h>
#include <arki/types/reftime.h>
#include <arki/types/product.h>
#include <arki/types/value.h>

#include <arki/utils/vm2.h>
#include <meteo-vm2/parser.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace scan {

namespace vm2 {

struct VM2Validator : public Validator
{
	virtual ~VM2Validator() {}

	// Validate data found in a file
	virtual void validate(int fd, off_t offset, size_t size, const std::string& fname) const
	{
		char buf[1024];

		if (pread(fd, buf, size, offset) == -1)
			throw wibble::exception::System(fname);

        std::string s((const char *)buf, size);

		wibble::Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
		if (!re.match(s)) 
			throw wibble::exception::Consistency("Not a valid VM2 file", s);
	}

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const
	{
		std::string s((const char *)buf, size);

		if (size == 0)
			throw wibble::exception::Consistency("Empty VM2 file");
        wibble::Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
		if (!re.match(s))
			throw wibble::exception::Consistency("Not a valid VM2 file", s);
	}
};

static VM2Validator vm_validator;

const Validator& validator() { return vm_validator; }

}

Vm2::Vm2() : in(0), parser(0) {}

Vm2::~Vm2()
{
    close();
}

void Vm2::open(const std::string& filename)
{
    string basedir, relname;
    utils::files::resolve_path(filename, basedir, relname);
    open(sys::abspath(filename), basedir, relname);
}

void Vm2::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
    // Close the previous file if needed
    close();
    this->filename = sys::abspath(filename);
    this->basedir = basedir;
    this->relname = relname;
    if (relname == "-") {
        this->in = &std::cin;
    } else {
        this->in = new std::ifstream(filename.c_str());
    }
    if (!in->good())
        throw wibble::exception::File(filename, "opening file for reading");
    parser = new meteo::vm2::Parser(*in);
}

void Vm2::close()
{
	if (in && relname != "-")
        delete in;
    if (parser) delete parser;
    in = 0;
    parser = 0;
}

bool Vm2::next(Metadata& md)
{
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

    md.clear();
    unique_ptr<source::Blob> source(source::Blob::create("vm2", basedir, relname, offset, size));
    md.set_cached_data(wibble::sys::Buffer(line.c_str(), line.size()));
    md.set_source(upcast<Source>(move(source)));
    md.add_note("Scanned from " + relname);
    md.set(Reftime::createPosition(Time(value.year, value.month, value.mday, value.hour, value.min, value.sec)));
    md.set(Area::createVM2(value.station_id));
    md.set(Product::createVM2(value.variable_id));

    // Look for the comma before the value starts
    size_t pos = 0;
    pos = line.find(',', pos);
    pos = line.find(',', pos + 1);
    pos = line.find(',', pos + 1);
    // Store the rest as a value
    md.set(types::Value::create(line.substr(pos+1)));

    return true;
}

wibble::sys::Buffer Vm2::reconstruct(const Metadata& md, const std::string& value)
{
    stringstream res;

    const reftime::Position* rt = md.get<reftime::Position>();
    const area::VM2* area = dynamic_cast<const area::VM2*>(md.get<Area>());
    const product::VM2* product = dynamic_cast<const product::VM2*>(md.get<Product>());

    res << setfill('0') << setw(4) << rt->time.vals[0]
        << setfill('0') << setw(2) << rt->time.vals[1]
        << setfill('0') << setw(2) << rt->time.vals[2]
        << setfill('0') << setw(2) << rt->time.vals[3]
        << setfill('0') << setw(2) << rt->time.vals[4];

    if (rt->time.vals[5] != 0)
        res << setfill('0') << setw(2) << rt->time.vals[5];

    res << "," << area->station_id()
        << "," << product->variable_id()
        << "," << value;

    return wibble::sys::Buffer(res.str().data(), res.str().size());
}

}
}
// vim:set ts=4 sw=4:
