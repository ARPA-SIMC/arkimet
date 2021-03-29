#include "summary.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include "files.h"
#include "structured.h"
#include "metadata.h"
#include "arki/summary.h"
#include "arki/summary/short.h"
#include "arki/stream.h"
#include "arki/structured/json.h"
#include "arki/structured/keys.h"
#include "arki/structured/memory.h"
#include "arki/utils/sys.h"
#include "arki/utils/geos.h"
#include "arki/core/binary.h"
#include "arki/formatter.h"
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_Summary_Type = nullptr;

}

namespace {

/*
 * Summary
 */

struct count : public Getter<count, arkipy_Summary>
{
    constexpr static const char* name = "count";
    constexpr static const char* doc = "Return the number of metadata described by this summary";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try {
            return to_python(self->summary->count());
        } ARKI_CATCH_RETURN_PYO;
    }
};

struct size : public Getter<size, arkipy_Summary>
{
    constexpr static const char* name = "size";
    constexpr static const char* doc = "Return the total size of all the metadata described by this summary";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try {
            return to_python((size_t)self->summary->size());
        } ARKI_CATCH_RETURN_PYO;
    }
};


struct add : public MethKwargs<add, arkipy_Summary>
{
    constexpr static const char* name = "add";
    constexpr static const char* signature = "val: Union[arki.Metadata, arki.Summary]";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "merge a metadata or summary into this summary";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "val", nullptr };
        PyObject* arg_val = Py_None;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &arg_val))
            return nullptr;

        try {
            if (arkipy_Metadata_Check(arg_val))
                self->summary->add(*((arkipy_Metadata*)arg_val)->md);
            else if (arkipy_Summary_Check(arg_val))
                self->summary->add(*((arkipy_Summary*)arg_val)->summary);
            else
            {
                PyErr_SetString(PyExc_TypeError, "Argument must be arki.Metadata or arki.Summary");
                return nullptr;
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct write : public MethKwargs<write, arkipy_Summary>
{
    constexpr static const char* name = "write";
    constexpr static const char* signature = "file: Union[int, BinaryIO], format: str = 'binary', annotate: bool = False";
    constexpr static const char* returns = "Optional[arki.cfg.Section]";
    constexpr static const char* summary = "write the summary to a file";
    constexpr static const char* doc = R"(
:param file: the output file. The file needs to be either an integer file or
             socket handle, or a file-like object with a fileno() method
             that returns an integer handle.
:param format: "binary", "yaml", or "json". Default: "binary".
:param annotate: if True, use a :class:`arkimet.Formatter` to add metadata
                 descriptions to the output
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", "format", "annotate", NULL };
        PyObject* arg_file = Py_None;
        const char* format = nullptr;
        int annotate = 0;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|sp", const_cast<char**>(kwlist), &arg_file, &format, &annotate))
            return nullptr;

        try {
            std::unique_ptr<StreamOutput> out = binaryio_stream_output(arg_file);

            if (!format || strcmp(format, "binary") == 0)
            {
                self->summary->write(*out);
            } else if (strcmp(format, "yaml") == 0) {
                std::unique_ptr<arki::Formatter> formatter;
                if (annotate)
                    formatter = arki::Formatter::create();
                std::string yaml = self->summary->to_yaml(formatter.get());
                out->send_buffer(yaml.data(), yaml.size());
            } else if (strcmp(format, "json") == 0) {
                std::unique_ptr<arki::Formatter> formatter;
                if (annotate)
                    formatter = arki::Formatter::create();
                std::stringstream buf;
                arki::structured::JSON output(buf);
                self->summary->serialise(output, arki::structured::keys_json, formatter.get());
                out->send_buffer(buf.str().data(), buf.str().size());
            } else {
                PyErr_Format(PyExc_ValueError, "Unsupported metadata serialization format: %s", format);
                return nullptr;
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct write_short : public MethKwargs<write_short, arkipy_Summary>
{
    constexpr static const char* name = "write_short";
    constexpr static const char* signature = "file: Union[int, BinaryIO], format: str='binary', annotate: bool = False";
    constexpr static const char* returns = "Optional[arki.cfg.Section]";
    constexpr static const char* summary = "write the short summary to a file";
    constexpr static const char* doc = R"(
:param file: the output file. The file needs to be either an integer file or
             socket handle, or a file-like object with a fileno() method
             that returns an integer handle.
:param format: "binary", "yaml", or "json". Default: "binary".
:param annotate: if True, use a :class:`arkimet.Formatter` to add metadata
                 descriptions to the output
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", "format", "annotate", NULL };
        PyObject* arg_file = Py_None;
        const char* format = nullptr;
        int annotate = 0;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|sp", const_cast<char**>(kwlist), &arg_file, &format, &annotate))
            return nullptr;

        try {
            BinaryOutputFile out(arg_file);

            std::unique_ptr<arki::Formatter> formatter;
            if (annotate)
                formatter = arki::Formatter::create();

            summary::Short shrt;
            self->summary->visit(shrt);

            if (!format || strcmp(format, "yaml") == 0)
            {
                std::stringstream buf;
                shrt.write_yaml(buf, formatter.get());
                if (out.fd)
                    out.fd->write_all_or_retry(buf.str().data(), buf.str().size());
                else
                    out.abstract->write(buf.str().data(), buf.str().size());
            } else if (strcmp(format, "json") == 0) {
                std::stringstream buf;
                arki::structured::JSON output(buf);
                shrt.serialise(output, arki::structured::keys_python, formatter.get());
                if (out.fd)
                    out.fd->write_all_or_retry(buf.str().data(), buf.str().size());
                else
                    out.abstract->write(buf.str().data(), buf.str().size());
            } else {
                PyErr_Format(PyExc_ValueError, "Unsupported metadata serialization format: %s", format);
                return nullptr;
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct to_python : public MethNoargs<to_python, arkipy_Summary>
{
    constexpr static const char* name = "to_python";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Dict[str, Any]";
    constexpr static const char* summary = "return the summary contents in a python dict";

    static PyObject* run(Impl* self)
    {
        try {
            PythonEmitter e;
            self->summary->serialise(e, arki::structured::keys_python);
            return e.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct get_convex_hull : public MethNoargs<get_convex_hull, arkipy_Summary>
{
    constexpr static const char* name = "get_convex_hull";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Optional[str]";
    constexpr static const char* summary = "compute the convex hull for this summary and return it as a WKT string";
    constexpr static const char* doc = R"(
None is returned if the convex hull could not be computed.
)";

    static PyObject* run(Impl* self)
    {
        try {
            // Compute bounding box, and store the WKT in bounding
            auto bbox = self->summary->getConvexHull();
            if (bbox.get())
            {
                pyo_unique_ptr res = arki::python::to_python(bbox->toString());
                return res.release();
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct read_binary : public ClassMethKwargs<read_binary>
{
    constexpr static const char* name = "read_binary";
    constexpr static const char* signature = "src: Union[bytes, ByteIO]";
    constexpr static const char* returns = "arkimet.Summary";
    constexpr static const char* summary = "Read a Summary from a binary file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "src", nullptr };
        PyObject* py_src = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_src))
            return nullptr;

        try {
            if (PyBytes_Check(py_src))
            {
                char* buffer;
                Py_ssize_t length;
                if (PyBytes_AsStringAndSize(py_src, &buffer, &length) == -1)
                    throw PythonException();

                core::BinaryDecoder dec((const uint8_t*)(buffer), length);
                std::unique_ptr<Summary> res(new Summary);
                res->read(dec, "bytes buffer");
                return (PyObject*)summary_create(std::move(res));
            } else {
                BinaryInputFile in(py_src);
                ReleaseGIL gil;

                std::unique_ptr<Summary> res(new Summary);

                if (in.fd)
                    res->read(*in.fd);
                else
                    res->read(*in.abstract);

                gil.lock();
                return (PyObject*)summary_create(std::move(res));
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct read_yaml : public ClassMethKwargs<read_yaml>
{
    constexpr static const char* name = "read_yaml";
    constexpr static const char* signature = "src: Union[str, StringIO, bytes, ByteIO]";
    constexpr static const char* returns = "arkimet.Summary";
    constexpr static const char* summary = "Read a Summary from a YAML file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "src", nullptr };
        PyObject* py_src = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_src))
            return nullptr;

        try {
            std::unique_ptr<Summary> res(new Summary);
            if (PyBytes_Check(py_src))
            {
                char* buffer;
                Py_ssize_t length;
                if (PyBytes_AsStringAndSize(py_src, &buffer, &length) == -1)
                    throw PythonException();
                ReleaseGIL gil;
                auto reader = arki::core::LineReader::from_chars(buffer, length);
                res->readYaml(*reader, "bytes buffer");
            } else if (PyUnicode_Check(py_src)) {
                Py_ssize_t length;
                const char* buffer = throw_ifnull(PyUnicode_AsUTF8AndSize(py_src, &length));
                ReleaseGIL gil;
                auto reader = arki::core::LineReader::from_chars(buffer, length);
                res->readYaml(*reader, "str buffer");
            } else if (PyObject_HasAttrString(py_src, "encoding")) {
                TextInputFile input(py_src);
                ReleaseGIL gil;

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

                res->readYaml(*reader, input_name);
            } else {
                BinaryInputFile input(py_src);
                ReleaseGIL gil;

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
                res->readYaml(*reader, input_name);
            }
            return (PyObject*)summary_create(std::move(res));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct read_json : public ClassMethKwargs<read_json>
{
    constexpr static const char* name = "read_json";
    constexpr static const char* signature = "src: Union[str, StringIO, bytes, ByteIO]";
    constexpr static const char* returns = "arkimet.Summary";
    constexpr static const char* summary = "Read a Summary from a JSON file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "src", nullptr };
        PyObject* py_src = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_src))
            return nullptr;

        try {
            arki::structured::Memory parsed;
            if (PyBytes_Check(py_src))
            {
                char* buffer;
                Py_ssize_t length;
                if (PyBytes_AsStringAndSize(py_src, &buffer, &length) == -1)
                    throw PythonException();
                auto input = arki::core::BufferedReader::from_chars(buffer, length);
                ReleaseGIL gil;
                arki::structured::JSON::parse(*input, parsed);
            } else if (PyUnicode_Check(py_src)) {
                Py_ssize_t length;
                const char* buffer = throw_ifnull(PyUnicode_AsUTF8AndSize(py_src, &length));
                auto input = arki::core::BufferedReader::from_chars(buffer, length);
                ReleaseGIL gil;
                arki::structured::JSON::parse(*input, parsed);
            } else if (PyObject_HasAttrString(py_src, "encoding")) {
                TextInputFile input(py_src);

                std::unique_ptr<arki::core::BufferedReader> reader;
                if (input.fd)
                    reader = arki::core::BufferedReader::from_fd(*input.fd);
                else
                    reader = arki::core::BufferedReader::from_abstract(*input.abstract);

                ReleaseGIL gil;
                arki::structured::JSON::parse(*reader, parsed);
            } else {
                BinaryInputFile input(py_src);

                std::unique_ptr<arki::core::BufferedReader> reader;
                if (input.fd)
                    reader = arki::core::BufferedReader::from_fd(*input.fd);
                else
                    reader = arki::core::BufferedReader::from_abstract(*input.abstract);

                ReleaseGIL gil;
                arki::structured::JSON::parse(*reader, parsed);
            }

            ReleaseGIL gil;
            std::unique_ptr<Summary> res(new Summary);
            res->read(arki::structured::keys_json, parsed.root());

            gil.lock();
            return (PyObject*)summary_create(std::move(res));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct SummaryDef : public Type<SummaryDef, arkipy_Summary>
{
    constexpr static const char* name = "Summary";
    constexpr static const char* qual_name = "arkimet.Summary";
    constexpr static const char* doc = R"(
Arkimet summary

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<count, size> getsetters;
    Methods<add, write, write_short, to_python, get_convex_hull, read_binary, read_yaml, read_json> methods;

    static void _dealloc(Impl* self)
    {
        delete self->summary;
        self->summary = nullptr;
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("Summary");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("Summary");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            self->summary = new Summary;
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

SummaryDef* summary_def = nullptr;

}

namespace arki {
namespace python {

arkipy_Summary* summary_create()
{
    return (arkipy_Summary*)throw_ifnull(PyObject_CallObject((PyObject*)arkipy_Summary_Type, NULL));
}

arkipy_Summary* summary_create(std::unique_ptr<Summary>&& summary)
{
    arkipy_Summary* result = PyObject_New(arkipy_Summary, arkipy_Summary_Type);
    if (!result) throw PythonException();
    result->summary = summary.release();
    return result;
}

void register_summary(PyObject* m)
{
    summary_def = new SummaryDef;
    summary_def->define(arkipy_Summary_Type, m);
}

}
}
