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
#include "arki/structured/json.h"
#include "arki/structured/keys.h"
#include "arki/utils/sys.h"
#include "arki/utils/geos.h"
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
    constexpr static const char* signature = "file: Union[int, BinaryIO], format: str='binary'";
    constexpr static const char* returns = "Optional[arki.cfg.Section]";
    constexpr static const char* summary = "write the summary to a file";
    constexpr static const char* doc = R"(
Arguments:
  file: the output file. The file needs to be either an integer file or
        socket handle, or a file-like object with a fileno() method
        that returns an integer handle.
  format: "binary", "yaml", or "json". Default: "binary".
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", "format", NULL };
        PyObject* arg_file = Py_None;
        const char* format = nullptr;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|s", const_cast<char**>(kwlist), &arg_file, &format))
            return nullptr;

        try {
            BinaryOutputFile out(arg_file);

            if (!format || strcmp(format, "binary") == 0)
            {
                if (out.fd)
                    self->summary->write(*out.fd);
                else
                    self->summary->write(*out.abstract);
            } else if (strcmp(format, "yaml") == 0) {
                std::string yaml = self->summary->to_yaml();
                if (out.fd)
                    out.fd->write_all_or_retry(yaml);
                else
                    out.abstract->write(yaml.data(), yaml.size());
                return nullptr;
            } else if (strcmp(format, "json") == 0) {
                std::stringstream buf;
                arki::structured::JSON output(buf);
                self->summary->serialise(output, structured::keys_json);
                if (out.fd)
                    out.fd->write_all_or_retry(buf.str());
                else
                    out.abstract->write(buf.str().data(), buf.str().size());
                return nullptr;
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
    constexpr static const char* signature = "file: Union[int, BinaryIO], format: str='binary'";
    constexpr static const char* returns = "Optional[arki.cfg.Section]";
    constexpr static const char* summary = "write the short summary to a file";
    constexpr static const char* doc = R"(
Arguments:
  file: the output file. The file needs to be either an integer file or
        socket handle, or a file-like object with a fileno() method
        that returns an integer handle.
  format: "binary", "yaml", or "json". Default: "binary".
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", "format", NULL };
        PyObject* arg_file = Py_None;
        const char* format = nullptr;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|s", const_cast<char**>(kwlist), &arg_file, &format))
            return nullptr;

        try {
            BinaryOutputFile out(arg_file);

            summary::Short shrt;
            self->summary->visit(shrt);

            if (!format || strcmp(format, "yaml") == 0)
            {
                std::stringstream buf;
                shrt.write_yaml(buf);
                if (out.fd)
                    out.fd->write_all_or_retry(buf.str().data(), buf.str().size());
                else
                    out.abstract->write(buf.str().data(), buf.str().size());
                return nullptr;
            } else if (strcmp(format, "json") == 0) {
                std::stringstream buf;
                arki::structured::JSON output(buf);
                shrt.serialise(output, structured::keys_python);
                if (out.fd)
                    out.fd->write_all_or_retry(buf.str().data(), buf.str().size());
                else
                    out.abstract->write(buf.str().data(), buf.str().size());
                return nullptr;
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
    constexpr static const char* doc = R"(
Arguments:
  file: the output file. The file needs to be either an integer file or
        socket handle, or a file-like object with a fileno() method
        that returns an integer handle.
  format: "binary", "yaml", or "json". Default: "binary".
)";

    static PyObject* run(Impl* self)
    {
        try {
            PythonEmitter e;
            self->summary->serialise(e, structured::keys_python);
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
    Methods<add, write, write_short, to_python, get_convex_hull> methods;

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


