#include "printer.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/formatter.h"
#include "arki/emitter/json.h"
#include "arki/utils/sys.h"
#include <sstream>
#include <ostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace metadata {

BinaryPrinter::BinaryPrinter(utils::sys::NamedFileDescriptor& out, const std::string& fname)
    : out(out), fname(fname)
{
}

BinaryPrinter::~BinaryPrinter()
{
}

bool BinaryPrinter::eat(unique_ptr<Metadata>&& md)
{
    md->write(out, out.name());
    return true;
}

bool BinaryPrinter::observe(const Metadata& md)
{
    md.write(out, out.name());
    return true;
}

bool BinaryPrinter::eat_summary(std::unique_ptr<Summary> s)
{
    s->write(out, out.name());
    return true;
}

bool BinaryPrinter::observe_summary(const Summary& s)
{
    s.write(out, out.name());
    return true;
}


SerializingPrinter::SerializingPrinter(utils::sys::NamedFileDescriptor& out, bool formatted)
    : out(out), formatter(0)
{
    if (formatted)
        formatter = Formatter::create().release();
}
SerializingPrinter::~SerializingPrinter()
{
    delete formatter;
}

bool SerializingPrinter::eat(unique_ptr<Metadata>&& md)
{
    string res = serialize(*md);
    out.write_all_or_throw(res.data(), res.size());
    return true;
}

bool SerializingPrinter::observe(const Metadata& md)
{
    string res = serialize(md);
    out.write_all_or_throw(res.data(), res.size());
    return true;
}

bool SerializingPrinter::eat_summary(unique_ptr<Summary> s)
{
    string res = serialize(*s);
    out.write_all_or_throw(res.data(), res.size());
    return true;
}

bool SerializingPrinter::observe_summary(const Summary& s)
{
    string res = serialize(s);
    out.write_all_or_throw(res.data(), res.size());
    return true;
}


std::string YamlPrinter::serialize(const Metadata& md)
{
    stringstream ss;
    md.writeYaml(ss, formatter);
    ss << endl;
    return ss.str();
}

std::string YamlPrinter::serialize(const Summary& s)
{
    stringstream ss;
    s.writeYaml(ss, formatter);
    ss << endl;
    return ss.str();
}


std::string JSONPrinter::serialize(const Metadata& md)
{
    stringstream ss;
    emitter::JSON json(ss);
    md.serialise(json, formatter);
    return ss.str();
}

std::string JSONPrinter::serialize(const Summary& s)
{
    stringstream ss;
    emitter::JSON json(ss);
    s.serialise(json, formatter);
    return ss.str();
}

}
}
