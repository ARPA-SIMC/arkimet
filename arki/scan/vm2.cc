#include "arki/scan/vm2.h"
#include "arki/exceptions.h"
#include "arki/segment.h"
#include "arki/core/time.h"
#include "arki/types/source.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/regexp.h"
#include "arki/scan/validator.h"
#include "arki/types/area.h"
#include "arki/types/reftime.h"
#include "arki/types/product.h"
#include "arki/types/value.h"
#include <cstring>
#include <iomanip>
#include <unistd.h>
#include <meteo-vm2/parser.h>
#include <ext/stdio_filebuf.h>

using namespace arki::types;
using namespace arki::utils;

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

class Input
{
public:
    std::string md_note;
    std::istream* in = nullptr;
    meteo::vm2::Parser* parser = nullptr;
    bool close = false;
    meteo::vm2::Value value;
    std::string line;
    off_t offset = 0;

    Input(const std::filesystem::path& abspath)
        : md_note("Scanned from " + abspath.filename().native()), close(true)
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

    /**
     * Read the next line/value from the input.
     *
     * Set value and line.
     *
     * Return true if a valid line was found, false on end of file.
     */
    bool next()
    {
        while (true)
        {
            try {
                if (!parser->next(value, line))
                    return false;
                else
                    break;
            } catch (std::exception& e) {
                nag::warning("Skipping VM2 line: %s", e.what());
            }
        }
        return true;
    }

    /**
     * Read the next line/value from the input.
     *
     * Set offset, value and line.
     *
     * Return true if a valid line was found, false on end of file.
     */
    bool next_with_offset()
    {
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
        return true;
    }

    void to_metadata(Metadata& md)
    {
        md.add_note(md_note);
        md.set(Reftime::createPosition(core::Time(value.year, value.month, value.mday, value.hour, value.min, value.sec)));
        md.set<area::VM2>(value.station_id);
        md.set(Product::createVM2(value.variable_id));
        store_value(md);
    }

    void store_value(Metadata& md)
    {
        // Look for the comma before the value starts
        size_t pos = 0;
        pos = line.find(',', pos);
        pos = line.find(',', pos + 1);
        pos = line.find(',', pos + 1);
        // Store the rest as a value
        md.set(types::Value::create(line.substr(pos+1)));
    }
};

}

Vm2::Vm2() {}

Vm2::~Vm2()
{
}

std::shared_ptr<Metadata> Vm2::scan_data(const std::vector<uint8_t>& data)
{
    std::istringstream str(std::string(data.begin(), data.end()));
    vm2::Input input(str);
    std::shared_ptr<Metadata> md(new Metadata);
    if (!input.next())
        throw std::runtime_error("input line did not look like a VM2 line");
    input.to_metadata(*md);
    md->set_source_inline("vm2", metadata::DataManager::get().to_data("vm2", std::vector<uint8_t>(input.line.begin(), input.line.end())));
    return md;
}

std::shared_ptr<Metadata> Vm2::scan_singleton(const std::filesystem::path& abspath)
{
    auto md = std::make_shared<Metadata>();
    vm2::Input input(abspath);
    if (!input.next())
        throw std::runtime_error(abspath.native() + " contains no VM2 data");
    input.to_metadata(*md);
    md->set_cached_data(metadata::DataManager::get().to_data("vm2", std::vector<uint8_t>(input.line.begin(), input.line.end())));

    if (input.next())
        throw std::runtime_error(abspath.native() + " contains more than one VM2 data");
    return md;
}

bool Vm2::scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest)
{
    using namespace std;
    // see https://stackoverflow.com/questions/2746168/how-to-construct-a-c-fstream-from-a-posix-file-descriptor#5253726
    __gnu_cxx::stdio_filebuf<char> filebuf(in, std::ios::in);
    istream is(&filebuf);
    vm2::Input input(is);
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!input.next()) break;
        input.to_metadata(*md);
        md->set_source_inline("vm2", metadata::DataManager::get().to_data("vm2", vector<uint8_t>(input.line.begin(), input.line.end())));
        if (!dest(move(md))) return false;
    }
    return true;
}

bool Vm2::scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    vm2::Input input(reader->segment().abspath);
    while (true)
    {
        std::unique_ptr<Metadata> md(new Metadata);
        if (!input.next_with_offset()) break;
        input.to_metadata(*md);
        md->set_source(Source::createBlob(reader, input.offset, input.line.size()));
        md->set_cached_data(metadata::DataManager::get().to_data("vm2", std::vector<uint8_t>(input.line.begin(), input.line.end())));
        if (!dest(move(md))) return false;
    }
    return true;
}

std::vector<uint8_t> Vm2::reconstruct(const Metadata& md, const std::string& value)
{
    using namespace std;
    std::stringstream res;

    const reftime::Position* rt = md.get<reftime::Position>();
    auto time = rt->get_Position();
    const area::VM2* area = dynamic_cast<const area::VM2*>(md.get<Area>());
    const Product* product = md.get<Product>();
    unsigned vi;
    product->get_VM2(vi);

    res << setfill('0') << setw(4) << time.ye
        << setfill('0') << setw(2) << time.mo
        << setfill('0') << setw(2) << time.da
        << setfill('0') << setw(2) << time.ho
        << setfill('0') << setw(2) << time.mi;

    if (time.se != 0)
        res << setfill('0') << setw(2) << time.se;

    auto sid = area->get_VM2();
    res << "," << sid
        << "," << vi
        << "," << value;

    string reconstructed = res.str();
    return vector<uint8_t>(reconstructed.begin(), reconstructed.end());
}

void Vm2::normalize_before_dispatch(Metadata& md)
{
    if (const Value* value = md.get<types::Value>())
    {
        auto orig = md.get_data().read();
        auto normalized = reconstruct(md, value->buffer);
        if (orig != normalized)
        {
            md.set_cached_data(metadata::DataManager::get().to_data("vm2", std::move(normalized)));
            md.makeInline();
        }
    }
}

}
}
