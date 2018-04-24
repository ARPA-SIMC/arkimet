#include "arki/scan/vm2.h"
#include "arki/exceptions.h"
#include "arki/core/time.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/runtime/config.h"
#include "arki/utils/files.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/regexp.h"
#include "arki/utils/lua.h"
#include "arki/scan/any.h"
#include "arki/types/area.h"
#include "arki/types/reftime.h"
#include "arki/types/product.h"
#include "arki/types/value.h"
#include "arki/utils/vm2.h"
#include <cstring>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <meteo-vm2/parser.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace scan {

namespace vm2 {

struct VM2Validator : public Validator
{
    std::string format() const override { return "VM2"; }

    // Validate data found in a file
    void validate_file(sys::NamedFileDescriptor& fd, off_t offset, size_t size) const override
    {
        if (size >= 1024)
            throw_check_error(fd, offset, "size of data to check (" + std::to_string(size) + ") is too long for a VM2 line");

        char buf[1024];
        size_t sz = fd.pread(buf, size, offset);
        std::string s((const char*)buf, sz);

        Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
        if (!re.match(s))
            throw_check_error(fd, offset, "not a valid VM2 line: '" + s + "'");
    }

    // Validate a memory buffer
    void validate_buf(const void* buf, size_t size) const override
    {
        std::string s((const char *)buf, size);

        if (size == 0)
            throw_check_error("buffer is empty");
        Regexp re(meteo::vm2::Parser::regexp_str, 0, REG_EXTENDED);
        if (!re.match(s))
            throw_check_error("not a valid VM2 line: '" + s + "'");
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

void Vm2::open(const std::string& filename, std::shared_ptr<segment::Reader> reader)
{
    Scanner::open(filename, reader);
    this->in = new std::ifstream(filename.c_str());
    if (!in->good())
        throw_file_error(filename, "cannot open file for reading");
    parser = new meteo::vm2::Parser(*in);
}

void Vm2::close()
{
    Scanner::close();
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
        } catch (std::exception& e) {
            nag::warning("Skipping VM2 line: %s", e.what());
        }
    }

    size_t size = line.size();

    md.clear();
    md.set_source(Source::createBlob("vm2", reader, offset, size));
    md.set_cached_data(vector<uint8_t>(line.begin(), line.end()));
    md.add_note("Scanned from " + str::basename(filename));
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

vector<uint8_t> Vm2::reconstruct(const Metadata& md, const std::string& value)
{
    stringstream res;

    const reftime::Position* rt = md.get<reftime::Position>();
    const area::VM2* area = dynamic_cast<const area::VM2*>(md.get<Area>());
    const product::VM2* product = dynamic_cast<const product::VM2*>(md.get<Product>());

    res << setfill('0') << setw(4) << rt->time.ye
        << setfill('0') << setw(2) << rt->time.mo
        << setfill('0') << setw(2) << rt->time.da
        << setfill('0') << setw(2) << rt->time.ho
        << setfill('0') << setw(2) << rt->time.mi;

    if (rt->time.se != 0)
        res << setfill('0') << setw(2) << rt->time.se;

    res << "," << area->station_id()
        << "," << product->variable_id()
        << "," << value;

    string reconstructed = res.str();
    return vector<uint8_t>(reconstructed.begin(), reconstructed.end());
}

}
}
