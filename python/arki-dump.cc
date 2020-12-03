#include "arki-dump.h"
#include "arki/utils/sys.h"
#include "arki/core/file.h"
#include "arki/core/binary.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/types.h"
#include "arki/types/source.h"
#include "arki/types/bundle.h"
#include "arki/utils/geos.h"
#include "arki/formatter.h"
#include "arki/nag.h"
#include "arki-dump.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include "files.h"
#include <sstream>

using namespace arki::python;
using namespace arki::utils;

extern "C" {

PyTypeObject* arkipy_ArkiDump_Type = nullptr;

}


namespace {

#ifdef HAVE_GEOS
// Add to \a s the info from all data read from \a in
template<typename Input>
void addToSummary(Input& in, arki::Summary& s)
{
    arki::Summary summary;

    arki::types::Bundle bundle;
    while (bundle.read_header(in))
    {
        if (bundle.signature == "MD" || bundle.signature == "!D")
        {
            if (!bundle.read_data(in)) break;
            arki::core::BinaryDecoder dec(bundle.data);
            auto md = arki::Metadata::read_binary_inner(dec, bundle.version, in.name());
            if (md->source().style() == arki::types::Source::Style::INLINE)
                md->read_inline_data(in);
            s.add(*md);
        }
        else if (bundle.signature == "SU")
        {
            if (!bundle.read_data(in)) break;
            arki::core::BinaryDecoder dec(bundle.data);
            summary.read_inner(dec, bundle.version, in.name());
            s.add(summary);
        }
        else if (bundle.signature == "MG")
        {
            if (!bundle.read_data(in)) break;
            arki::core::BinaryDecoder dec(bundle.data);
            arki::Metadata::read_group(dec, bundle.version, in.name(), [&](std::shared_ptr<arki::Metadata> md) { s.add(*md); return true; });
        }
        else
            throw std::runtime_error(in.name() + ": metadata entry does not start with 'MD', '!D', 'SU', or 'MG'");
    }
}
#endif


struct bbox : public MethKwargs<bbox, arkipy_ArkiDump>
{
    constexpr static const char* name = "bbox";
    constexpr static const char* signature = "input: str, output: str";
    constexpr static const char* returns = "str";
    constexpr static const char* summary = "run arki-dump --bbox";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "input", nullptr };
        PyObject* py_input = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_input))
            return nullptr;

        try {
            BinaryInputFile input(py_input);
            ReleaseGIL rg;

            // Read everything into a single summary
            arki::Summary summary;
            if (input.fd)
                addToSummary(*input.fd, summary);
            else
                addToSummary(*input.abstract, summary);

            // Get the bounding box
            auto hull = summary.getConvexHull();

            rg.lock();

            // Print it out
            std::string bbox;
            if (hull.get())
                return to_python(hull->toString());
            else
                Py_RETURN_NONE;
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
        PyObject* py_input = nullptr;
        PyObject* py_output = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OO", const_cast<char**>(kwlist), &py_input, &py_output))
            return nullptr;

        try {
            BinaryInputFile input(py_input);
            BinaryOutputFile output(py_output);

            ReleaseGIL rg;

            std::unique_ptr<arki::core::LineReader> reader;
            std::string input_name;
            if (input.fd)
            {
                input_name = input.fd->name();
                reader = arki::core::LineReader::from_fd(*input.fd);
            }
            else
            {
                input_name = input.abstract->name();
                reader = arki::core::LineReader::from_abstract(*input.abstract);
            }
            if (output.fd)
                while (auto md = arki::Metadata::read_yaml(*reader, input_name))
                    md->write(*output.fd);
            else
                while (auto md = arki::Metadata::read_yaml(*reader, input_name))
                    md->write(*output.abstract);

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
        PyObject* py_input = nullptr;
        PyObject* py_output = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OO", const_cast<char**>(kwlist), &py_input, &py_output))
            return nullptr;

        try {
            BinaryInputFile input(py_input);
            BinaryOutputFile output(py_output);

            ReleaseGIL rg;

            arki::Summary summary;
            std::unique_ptr<arki::core::LineReader> reader;
            std::string input_name;
            if (input.fd)
            {
                input_name = input.fd->name();
                reader = arki::core::LineReader::from_fd(*input.fd);
            }
            else
            {
                input_name = input.abstract->name();
                reader = arki::core::LineReader::from_abstract(*input.abstract);
            }
            if (output.fd)
                while (summary.readYaml(*reader, input_name))
                    summary.write(*output.fd);
            else
                while (summary.readYaml(*reader, input_name))
                    summary.write(*output.abstract);

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
        PyObject* py_input = nullptr;
        PyObject* py_output = nullptr;
        int annotate = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OO|p", const_cast<char**>(kwlist), &py_input, &py_output, &annotate))
            return nullptr;

        try {
            BinaryInputFile input(py_input);
            BinaryOutputFile output(py_output);

            ReleaseGIL rg;

            std::shared_ptr<arki::Formatter> formatter;
            if (annotate)
                formatter = arki::Formatter::create();

            std::function<void(const arki::Metadata&)> print_md;
            std::function<void(const arki::Summary&)> print_summary;

            if (output.fd)
            {
                print_md = [&output, formatter](const arki::Metadata& md) {
                    std::string res = md.to_yaml(formatter.get());
                    res += "\n";
                    output.fd->write_all_or_throw(res.data(), res.size());
                };
                print_summary = [&output, formatter](const arki::Summary& md) {
                    std::string res = md.to_yaml(formatter.get());
                    res += "\n";
                    output.fd->write_all_or_throw(res.data(), res.size());
                };
            } else {
                print_md = [&output, formatter](const arki::Metadata& md) {
                    std::string res = md.to_yaml(formatter.get());
                    res += "\n";
                    output.abstract->write(res.data(), res.size());
                };
                print_summary = [&output, formatter](const arki::Summary& md) {
                    std::string res = md.to_yaml(formatter.get());
                    res += "\n";
                    output.abstract->write(res.data(), res.size());
                };
            }

            arki::Summary summary;

            arki::types::Bundle bundle;
            std::string input_name;
            std::function<bool()> read_header;
            std::function<bool()> read_data;
            std::function<void(arki::Metadata& md)> read_inline_data;
            if (input.fd)
            {
                input_name = input.fd->name();
                read_header = [&bundle, &input] {
                    return bundle.read_header(*input.fd);
                };
                read_data = [&bundle, &input] {
                    return bundle.read_data(*input.fd);
                };
                read_inline_data = [&input](arki::Metadata& md) {
                    md.read_inline_data(*input.fd);
                };
            }
            else
            {
                input_name = input.abstract->name();
                read_header = [&bundle, &input] {
                    return bundle.read_header(*input.abstract);
                };
                read_data = [&bundle, &input] {
                    return bundle.read_data(*input.abstract);
                };
                read_inline_data = [&input](arki::Metadata& md) {
                    md.read_inline_data(*input.abstract);
                };
            }

            while (read_header())
            {
                if (bundle.signature == "MD" || bundle.signature == "!D")
                {
                    if (!read_data()) break;
                    arki::core::BinaryDecoder dec(bundle.data);
                    auto md = arki::Metadata::read_binary_inner(dec, bundle.version, input_name);
                    if (md->source().style() == arki::types::Source::Style::INLINE)
                        read_inline_data(*md);
                    print_md(*md);
                }
                else if (bundle.signature == "SU")
                {
                    if (!read_data()) break;
                    arki::core::BinaryDecoder dec(bundle.data);
                    summary.read_inner(dec, bundle.version, input_name);
                    print_summary(summary);
                }
                else if (bundle.signature == "MG")
                {
                    if (!read_data()) break;
                    arki::core::BinaryDecoder dec(bundle.data);
                    arki::Metadata::read_group(dec, bundle.version, input_name, [&](std::shared_ptr<arki::Metadata> md) { print_md(*md); return true; });
                }
                else
                    throw std::runtime_error(input_name + ": metadata entry does not start with 'MD', '!D', 'SU', or 'MG'");
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
