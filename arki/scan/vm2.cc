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
#include "arki/scan/validator.h"
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

struct Input
{
    std::istream* in = nullptr;
    meteo::vm2::Parser* parser = nullptr;
    bool close = false;

    Input(const std::string& abspath)
        : close(true)
    {
        in = new std::ifstream(abspath.c_str());
        if (!in->good())
            throw_file_error(abspath, "cannot open file for reading");
        parser = new meteo::vm2::Parser(*in);
    }

    Input(std::istream& st)
        : in(&st), close(false)
    {
        parser = new meteo::vm2::Parser(*in);
    }

    ~Input()
    {
        delete parser;
        if (close)
            delete in;
    }
};

}

Vm2::Vm2() {}

Vm2::~Vm2()
{
    close();
}

void Vm2::open(const std::string& filename, std::shared_ptr<segment::Reader> reader)
{
    close();
    this->filename = filename;
    this->reader = reader;
    delete input;
    input = nullptr;
    input = new vm2::Input(filename);
}

void Vm2::close()
{
    filename.clear();
    reader.reset();
    delete input;
    input = nullptr;
}

bool Vm2::scan_stream(vm2::Input& input, Metadata& md)
{
    meteo::vm2::Value value;
    std::string line;

    off_t offset = 0;
    while (true)
    {
        offset = input.in->tellg();
        try {
            if (!input.parser->next(value, line))
                return false;
            else
                break;
        } catch (std::exception& e) {
            nag::warning("Skipping VM2 line: %s", e.what());
        }
    }

    size_t size = line.size();

    md.clear();
    if (reader)
    {
        md.set_source(Source::createBlob(reader, offset, size));
        md.set_cached_data(vector<uint8_t>(line.begin(), line.end()));
    } else
        md.set_source_inline("bufr", vector<uint8_t>(line.begin(), line.end()));
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

bool Vm2::next(Metadata& md)
{
    return scan_stream(*input, md);
}

std::unique_ptr<Metadata> Vm2::scan_data(const std::vector<uint8_t>& data)
{
    std::istringstream str(std::string(data.begin(), data.end()));
    vm2::Input input(str);
    std::unique_ptr<Metadata> md(new Metadata);
    if (!scan_stream(input, *md))
        throw std::runtime_error("input line did not look like a VM2 line");
    return md;
}

bool Vm2::scan_file_inline(const std::string& abspath, metadata_dest_func dest)
{
    open(abspath, std::shared_ptr<segment::Reader>());
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!next(*md)) break;
        if (!dest(move(md))) return false;
    }
    return true;
}

bool Vm2::scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest)
{
    throw std::runtime_error("scan_pipe not yet implemented for VM2");
}

bool Vm2::scan_file(const std::string& abspath, std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    open(abspath, reader);
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!next(*md)) break;
        if (!dest(move(md))) return false;
    }
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
