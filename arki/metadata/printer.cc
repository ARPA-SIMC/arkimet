#include "printer.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/formatter.h"
#include "arki/emitter/json.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace metadata {

BinaryPrinter::BinaryPrinter(Output& out, const std::string& fname)
    : out(out), fname(fname)
{
}

BinaryPrinter::~BinaryPrinter()
{
}

bool BinaryPrinter::eat(unique_ptr<Metadata>&& md)
{
    md->write(out.stream(), out.name());
    return true;
}

bool BinaryPrinter::observe(const Metadata& md)
{
    md.write(out.stream(), out.name());
    return true;
}

bool BinaryPrinter::eat_summary(std::unique_ptr<Summary> s)
{
    s->write(out.stream(), out.name());
    return true;
}

bool BinaryPrinter::observe_summary(const Summary& s)
{
    s.write(out.stream(), out.name());
    return true;
}


YamlPrinter::YamlPrinter(Output& out, bool formatted)
    : out(out), formatter(0)
{
    if (formatted)
        formatter = Formatter::create();
}
YamlPrinter::~YamlPrinter()
{
    delete formatter;
}

bool YamlPrinter::eat(unique_ptr<Metadata>&& md)
{
    md->writeYaml(out.stream(), formatter);
    out.stream() << endl;
    return true;
}

bool YamlPrinter::observe(const Metadata& md)
{
    md.writeYaml(out.stream(), formatter);
    out.stream() << endl;
    return true;
}

bool YamlPrinter::eat_summary(unique_ptr<Summary> s)
{
    s->writeYaml(out.stream(), formatter);
    out.stream() << endl;
    return true;
}

bool YamlPrinter::observe_summary(const Summary& s)
{
    s.writeYaml(out.stream(), formatter);
    out.stream() << endl;
    return true;
}


JSONPrinter::JSONPrinter(Output& out, bool formatted)
    : formatter(0), json(new emitter::JSON(out.stream()))
{
    if (formatted)
        formatter = Formatter::create();
}
JSONPrinter::~JSONPrinter()
{
    delete formatter;
    delete json;
}

bool JSONPrinter::eat(unique_ptr<Metadata>&& md)
{
    md->serialise(*json, formatter);
    return true;
}

bool JSONPrinter::observe(const Metadata& md)
{
    md.serialise(*json, formatter);
    return true;
}

bool JSONPrinter::eat_summary(unique_ptr<Summary> s)
{
    s->serialise(*json, formatter);
    return true;
}

bool JSONPrinter::observe_summary(const Summary& s)
{
    s.serialise(*json, formatter);
    return true;
}

}
}
