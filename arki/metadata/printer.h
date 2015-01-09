#ifndef ARKI_METADATA_PRINTER_H
#define ARKI_METADATA_PRINTER_H

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
#include <arki/core.h>
#include <arki/metadata/consumer.h>

namespace arki {
class Metadata;
class Summary;

namespace emitter {
class JSON;
}

namespace metadata {

struct Printer : public Eater, public Observer
{
    virtual bool eat_summary(std::auto_ptr<Summary> s) = 0;
    virtual bool observe_summary(const Summary& s) = 0;

    virtual std::string describe() const = 0;
};

struct BinaryPrinter : public Printer
{
    Output& out;
    std::string fname;

    BinaryPrinter(Output& out, const std::string& fname=std::string());
    ~BinaryPrinter();

    std::string describe() const override { return "binary"; }

    bool eat(std::auto_ptr<Metadata> md) override;
    bool observe(const Metadata& md) override;
    bool eat_summary(std::auto_ptr<Summary> s) override;
    bool observe_summary(const Summary& s) override;
};

struct YamlPrinter : public Printer
{
    Output& out;
    Formatter* formatter;

    YamlPrinter(Output& out, bool formatted=false);
    ~YamlPrinter();

    std::string describe() const override { return "yaml"; }

    bool eat(std::auto_ptr<Metadata> md) override;
    bool observe(const Metadata& md) override;
    bool eat_summary(std::auto_ptr<Summary> s) override;
    bool observe_summary(const Summary& s) override;
};

struct JSONPrinter : public Printer
{
    emitter::JSON* json;
    Formatter* formatter;

    JSONPrinter(Output& out, bool formatted=false);
    ~JSONPrinter();

    std::string describe() const override { return "json"; }

    bool eat(std::auto_ptr<Metadata> md) override;
    bool observe(const Metadata& md) override;
    bool eat_summary(std::auto_ptr<Summary> s) override;
    bool observe_summary(const Summary& s) override;
};

}
}
#endif
