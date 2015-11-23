/*
 * metadata/printer - Metadata consumers to write Metadata to an output stream
 *
 * Copyright (C) 2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "printer.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/formatter.h"
#include "arki/emitter/json.h"

using namespace std;
using namespace wibble;
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
