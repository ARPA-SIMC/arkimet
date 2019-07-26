#include "arki-dump.h"
#include "arki/utils/sys.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/binary.h"
#include "arki/types.h"
#include "arki/types/source.h"
#include "arki/utils/geos.h"
#include "arki/formatter.h"
#include "arki-dump.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include <sstream>

using namespace arki::python;
using namespace arki::utils;

extern "C" {

PyTypeObject* arkipy_ArkiDump_Type = nullptr;

}


namespace {

std::unique_ptr<sys::NamedFileDescriptor> make_input(const std::string& input)
{
    if (input == "-")
        return std::unique_ptr<sys::NamedFileDescriptor>(new arki::core::Stdin);

    return std::unique_ptr<sys::NamedFileDescriptor>(new sys::File(input, O_RDONLY));
}

std::unique_ptr<sys::NamedFileDescriptor> make_output(const std::string& output)
{
    if (output == "-")
        return std::unique_ptr<sys::NamedFileDescriptor>(new arki::core::Stdout);
    else
        return std::unique_ptr<sys::NamedFileDescriptor>(new sys::File(output, O_WRONLY | O_CREAT | O_TRUNC, 0666));
}

#ifdef HAVE_GEOS
// Add to \a s the info from all data read from \a in
void addToSummary(sys::NamedFileDescriptor& in, arki::Summary& s)
{
    arki::Metadata md;
    arki::Summary summary;

    arki::types::Bundle bundle;
    while (bundle.read_header(in))
    {
        if (bundle.signature == "MD" || bundle.signature == "!D")
        {
            if (!bundle.read_data(in)) break;
            arki::BinaryDecoder dec(bundle.data);
            md.read_inner(dec, bundle.version, in.name());
            if (md.source().style() == arki::types::Source::INLINE)
                md.read_inline_data(in);
            s.add(md);
        }
        else if (bundle.signature == "SU")
        {
            if (!bundle.read_data(in)) break;
            arki::BinaryDecoder dec(bundle.data);
            summary.read_inner(dec, bundle.version, in.name());
            s.add(summary);
        }
        else if (bundle.signature == "MG")
        {
            if (!bundle.read_data(in)) break;
            arki::BinaryDecoder dec(bundle.data);
            arki::Metadata::read_group(dec, bundle.version, in.name(), [&](std::unique_ptr<arki::Metadata> md) { s.add(*md); return true; });
        }
        else
            throw std::runtime_error(in.name() + ": metadata entry does not start with 'MD', '!D', 'SU', or 'MG'");
    }
}
#endif

struct YamlPrinter
{
    sys::NamedFileDescriptor& out;
    arki::Formatter* formatter = nullptr;

    YamlPrinter(sys::NamedFileDescriptor& out, bool formatted=false)
        : out(out)
    {
        if (formatted)
            formatter = arki::Formatter::create().release();
    }
    ~YamlPrinter()
    {
        delete formatter;
    }

    bool print(const arki::Metadata& md)
    {
        std::string res = serialize(md);
        out.write_all_or_throw(res.data(), res.size());
        return true;
    }

    bool print_summary(const arki::Summary& s)
    {
        std::string res = serialize(s);
        out.write_all_or_throw(res.data(), res.size());
        return true;
    }

    std::string serialize(const arki::Metadata& md)
    {
        std::stringstream ss;
        md.write_yaml(ss, formatter);
        ss << std::endl;
        return ss.str();
    }

    std::string serialize(const arki::Summary& s)
    {
        std::stringstream ss;
        s.write_yaml(ss, formatter);
        ss << std::endl;
        return ss.str();
    }
};


struct bbox : public MethKwargs<bbox, arkipy_ArkiDump>
{
    constexpr static const char* name = "bbox";
    constexpr static const char* signature = "input: str, output: str";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-dump --bbox";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", "output", nullptr };
        const char* input = nullptr;
        const char* output = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "ss", const_cast<char**>(kwlist), &input, &output))
            return nullptr;

        try {
            ReleaseGIL rg;
            // Open the input file
            auto in = make_input(input);

            // Read everything into a single summary
            arki::Summary summary;
            addToSummary(*in, summary);

            // Get the bounding box
            auto hull = summary.getConvexHull();

            // Print it out
            std::string out;
            if (hull.get())
                out = hull->toString();
            else
                out = "no bounding box could be computed.";

            // Open the output file
            std::unique_ptr<sys::NamedFileDescriptor> outfd(make_output(output));
            outfd->write_all_or_throw(out);
            outfd->close();

            return throw_ifnull(PyLong_FromLong(0));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct reverse_data : public MethKwargs<reverse_data, arkipy_ArkiDump>
{
    constexpr static const char* name = "reverse_data";
    constexpr static const char* signature = "input: str, output: str";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-dump --from-yaml-data";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", "output", nullptr };
        const char* input = nullptr;
        const char* output = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "ss", const_cast<char**>(kwlist), &input, &output))
            return nullptr;

        try {
            ReleaseGIL rg;
            // Open the input file
            auto in = make_input(input);

            // Open the output channel
            std::unique_ptr<sys::NamedFileDescriptor> outfd(make_output(output));

            arki::Metadata md;
            auto reader = arki::core::LineReader::from_fd(*in);
            while (md.readYaml(*reader, in->name()))
                md.write(*outfd);

            return throw_ifnull(PyLong_FromLong(0));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct reverse_summary : public MethKwargs<reverse_summary, arkipy_ArkiDump>
{
    constexpr static const char* name = "reverse_summary";
    constexpr static const char* signature = "input: str, output: str";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-dump --from-yaml-summary";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", "output", nullptr };
        const char* input = nullptr;
        const char* output = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "ss", const_cast<char**>(kwlist), &input, &output))
            return nullptr;

        try {
            ReleaseGIL rg;
            // Open the input file
            auto in = make_input(input);

            // Open the output channel
            std::unique_ptr<sys::NamedFileDescriptor> outfd(make_output(output));

            arki::Summary summary;
            auto reader = arki::core::LineReader::from_fd(*in);
            while (summary.readYaml(*reader, in->name()))
                summary.write(*outfd);

            return throw_ifnull(PyLong_FromLong(0));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct dump_yaml : public MethKwargs<dump_yaml, arkipy_ArkiDump>
{
    constexpr static const char* name = "dump_yaml";
    constexpr static const char* signature = "input: str, output: str";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-dump [--annotate]";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", "output", "annotate", nullptr };
        const char* input = nullptr;
        const char* output = nullptr;
        int annotate = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "ss|p", const_cast<char**>(kwlist), &input, &output, &annotate))
            return nullptr;

        try {
            ReleaseGIL rg;
            // Open the input file
            auto in = make_input(input);

            // Open the output channel
            std::unique_ptr<sys::NamedFileDescriptor> outfd(make_output(output));

            YamlPrinter writer(*outfd, annotate);

            arki::Metadata md;
            arki::Summary summary;

            arki::types::Bundle bundle;
            while (bundle.read_header(*in))
            {
                if (bundle.signature == "MD" || bundle.signature == "!D")
                {
                    if (!bundle.read_data(*in)) break;
                    arki::BinaryDecoder dec(bundle.data);
                    md.read_inner(dec, bundle.version, in->name());
                    if (md.source().style() == arki::types::Source::INLINE)
                        md.read_inline_data(*in);
                    writer.print(md);
                }
                else if (bundle.signature == "SU")
                {
                    if (!bundle.read_data(*in)) break;
                    arki::BinaryDecoder dec(bundle.data);
                    summary.read_inner(dec, bundle.version, in->name());
                    writer.print_summary(summary);
                }
                else if (bundle.signature == "MG")
                {
                    if (!bundle.read_data(*in)) break;
                    arki::BinaryDecoder dec(bundle.data);
                    arki::Metadata::read_group(dec, bundle.version, in->name(), [&](std::unique_ptr<arki::Metadata> md) { return writer.print(*md); });
                }
                else
                    throw std::runtime_error(in->name() + ": metadata entry does not start with 'MD', '!D', 'SU', or 'MG'");
            }

            return throw_ifnull(PyLong_FromLong(0));
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiDumpDef : public Type<ArkiDumpDef, arkipy_ArkiDump>
{
    constexpr static const char* name = "ArkiDump";
    constexpr static const char* qual_name = "arkimet.ArkiDump";
    constexpr static const char* doc = R"(
arki-dump implementation
)";
    GetSetters<> getsetters;
    Methods<bbox, reverse_data, reverse_summary, dump_yaml> methods;

    static void _dealloc(Impl* self)
    {
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString(name);
    }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += " object";
        return PyUnicode_FromString(res.c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiDumpDef* arki_dump_def = nullptr;

}

namespace arki {
namespace python {

void register_arki_dump(PyObject* m)
{
    arki_dump_def = new ArkiDumpDef;
    arki_dump_def->define(arkipy_ArkiDump_Type, m);

}

}
}
