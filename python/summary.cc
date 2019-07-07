#include <Python.h>
#include "summary.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include "arki/summary.h"
#include "arki/summary/short.h"
#include "arki/emitter/json.h"
#include "arki/utils/sys.h"
#include "arki/utils/geos.h"
#include "config.h"
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
            int fd = file_get_fileno(arg_file);
            if (fd == -1) return nullptr;
            string fd_name;
            if (object_repr(arg_file, fd_name) == -1)
                return nullptr;

            if (!format || strcmp(format, "binary") == 0)
            {
                self->summary->write(fd, fd_name);
            } else if (strcmp(format, "yaml") == 0) {
                std::stringstream buf;
                self->summary->write_yaml(buf);
                sys::NamedFileDescriptor outfd(fd, fd_name);
                outfd.write_all_or_retry(buf.str());
                return nullptr;
            } else if (strcmp(format, "json") == 0) {
                std::stringstream buf;
                arki::emitter::JSON output(buf);
                self->summary->serialise(output);
                sys::NamedFileDescriptor outfd(fd, fd_name);
                outfd.write_all_or_retry(buf.str());
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
            int fd = file_get_fileno(arg_file);
            if (fd == -1) return nullptr;
            string fd_name;
            if (object_repr(arg_file, fd_name) == -1)
                return nullptr;

            summary::Short shrt;
            self->summary->visit(shrt);

            if (!format || strcmp(format, "yaml") == 0)
            {
                std::stringstream buf;
                shrt.write_yaml(buf);
                sys::NamedFileDescriptor outfd(fd, fd_name);
                outfd.write_all_or_retry(buf.str());
                return nullptr;
            } else if (strcmp(format, "json") == 0) {
                std::stringstream buf;
                arki::emitter::JSON output(buf);
                shrt.serialise(output);
                sys::NamedFileDescriptor outfd(fd, fd_name);
                outfd.write_all_or_retry(buf.str());
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
            self->summary->serialise(e);
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
    GetSetters<> getsetters;
    Methods<write, write_short, to_python, get_convex_hull> methods;

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
    return (arkipy_Summary*)PyObject_CallObject((PyObject*)arkipy_Summary_Type, NULL);
}

arkipy_Summary* summary_create(std::unique_ptr<Summary>&& summary)
{
    arkipy_Summary* result = PyObject_New(arkipy_Summary, arkipy_Summary_Type);
    if (!result) return nullptr;
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


