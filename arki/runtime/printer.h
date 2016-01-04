#ifndef ARKI_METADATA_PRINTER_H
#define ARKI_METADATA_PRINTER_H

/// Metadata consumers to write Metadata to an output stream

#include <arki/utils/sys.h>

namespace arki {
class Metadata;
class Summary;
class Formatter;

namespace emitter {
class JSON;
}

namespace metadata {

struct Printer
{
    virtual bool eat(std::unique_ptr<Metadata>&& md) = 0;
    virtual bool eat_summary(std::unique_ptr<Summary> s) = 0;
    virtual bool observe(const Metadata& md) = 0;
    virtual bool observe_summary(const Summary& s) = 0;

    virtual std::string describe() const = 0;
};

struct BinaryPrinter : public Printer
{
    utils::sys::NamedFileDescriptor& out;
    std::string fname;

    BinaryPrinter(utils::sys::NamedFileDescriptor& out, const std::string& fname=std::string());
    ~BinaryPrinter();

    std::string describe() const override { return "binary"; }

    bool eat(std::unique_ptr<Metadata>&& md) override;
    bool observe(const Metadata& md) override;
    bool eat_summary(std::unique_ptr<Summary> s) override;
    bool observe_summary(const Summary& s) override;
};

struct SerializingPrinter : public Printer
{
    utils::sys::NamedFileDescriptor& out;
    Formatter* formatter;

    SerializingPrinter(utils::sys::NamedFileDescriptor& out, bool formatted=false);
    ~SerializingPrinter();

    virtual std::string serialize(const Metadata& md) = 0;
    virtual std::string serialize(const Summary& s) = 0;

    bool eat(std::unique_ptr<Metadata>&& md) override;
    bool observe(const Metadata& md) override;
    bool eat_summary(std::unique_ptr<Summary> s) override;
    bool observe_summary(const Summary& s) override;
};

struct YamlPrinter : public SerializingPrinter
{
    using SerializingPrinter::SerializingPrinter;

    std::string describe() const override { return "yaml"; }

    std::string serialize(const Metadata& md);
    std::string serialize(const Summary& s);
};

struct JSONPrinter : public SerializingPrinter
{
    using SerializingPrinter::SerializingPrinter;
    Formatter* formatter;

    std::string describe() const override { return "json"; }

    std::string serialize(const Metadata& md);
    std::string serialize(const Summary& s);
};

}
}
#endif
