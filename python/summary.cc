#include <Python.h>
#include "summary.h"
#include "common.h"
#include "arki/summary.h"
#include "arki/summary/short.h"
#include "arki/emitter/json.h"
#include "arki/utils/sys.h"
#include "config.h"
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::python;

extern "C" {

/*
 * Summary
 */

static PyObject* arkipy_Summary_write(arkipy_Summary* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "file", "format", NULL };
    PyObject* arg_file = Py_None;
    const char* format = nullptr;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|s", (char**)kwlist, &arg_file, &format))
        return nullptr;

    int fd = file_get_fileno(arg_file);
    if (fd == -1) return nullptr;
    string fd_name;
    if (object_repr(arg_file, fd_name) == -1) return nullptr;

    try {
        if (!format || strcmp(format, "binary") == 0)
        {
            self->summary->write(fd, fd_name);
        } else if (strcmp(format, "yaml") == 0) {
            std::stringstream buf;
            self->summary->writeYaml(buf);
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

static PyObject* arkipy_Summary_write_short(arkipy_Summary* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "file", "format", NULL };
    PyObject* arg_file = Py_None;
    const char* format = nullptr;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|s", (char**)kwlist, &arg_file, &format))
        return nullptr;

    int fd = file_get_fileno(arg_file);
    if (fd == -1) return nullptr;
    string fd_name;
    if (object_repr(arg_file, fd_name) == -1) return nullptr;

    try {
        summary::Short shrt;
        self->summary->visit(shrt);

        if (!format || strcmp(format, "yaml") == 0)
        {
            std::stringstream buf;
            shrt.writeYaml(buf);
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

static PyObject* arkipy_Summary_to_python(arkipy_Summary* self, PyObject *null)
{
    try {
        PythonEmitter e;
        self->summary->serialise(e);
        return e.release();
    } ARKI_CATCH_RETURN_PYO
}


static PyMethodDef arkipy_Summary_methods[] = {
    {"write", (PyCFunction)arkipy_Summary_write, METH_VARARGS | METH_KEYWORDS, R"(
        Write the summary to a file.

        Arguments:
          file: the output file. The file needs to be either an integer file or
                socket handle, or a file-like object with a fileno() method
                that returns an integer handle.
          format: "binary", "yaml", or "json". Default: "binary".
        )" },
    {"write_short", (PyCFunction)arkipy_Summary_write_short, METH_VARARGS | METH_KEYWORDS, R"(
        Write the short summary to a file.

        Arguments:
          file: the output file. The file needs to be either an integer file or
                socket handle, or a file-like object with a fileno() method
                that returns an integer handle.
          format: "yaml", or "json". Default: "yaml".
        )" },
    {"to_python", (PyCFunction)arkipy_Summary_to_python, METH_NOARGS, R"(
        Return the summary contents in a python dict
        )" },
    {NULL}
};

static int arkipy_Summary_init(arkipy_Summary* self, PyObject* args, PyObject* kw)
{
    static const char* kwlist[] = { nullptr };
    if (!PyArg_ParseTupleAndKeywords(args, kw, "", (char**)kwlist))
        return -1;

    try {
        self->summary = new Summary;
        return 0;
    } ARKI_CATCH_RETURN_INT;
}

static void arkipy_Summary_dealloc(arkipy_Summary* self)
{
    delete self->summary;
    self->summary = nullptr;
}

static PyObject* arkipy_Summary_str(arkipy_Summary* self)
{
    return PyUnicode_FromFormat("Summary");
}

static PyObject* arkipy_Summary_repr(arkipy_Summary* self)
{
    return PyUnicode_FromFormat("Summary");
}

PyTypeObject arkipy_Summary_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "arkimet.Summary",   // tp_name
    sizeof(arkipy_Summary), // tp_basicsize
    0,                         // tp_itemsize
    (destructor)arkipy_Summary_dealloc, // tp_dealloc
    0,                         // tp_print
    0,                         // tp_getattr
    0,                         // tp_setattr
    0,                         // tp_compare
    (reprfunc)arkipy_Summary_repr, // tp_repr
    0,                         // tp_as_number
    0,                         // tp_as_sequence
    0,                         // tp_as_mapping
    0,                         // tp_hash
    0,                         // tp_call
    (reprfunc)arkipy_Summary_str,  // tp_str
    0,                         // tp_getattro
    0,                         // tp_setattro
    0,                         // tp_as_buffer
    Py_TPFLAGS_DEFAULT,        // tp_flags
    R"(
        Arkimet summary

        TODO: document

        Examples::

            TODO: add examples
    )",                        // tp_doc
    0,                         // tp_traverse
    0,                         // tp_clear
    0,                         // tp_richcompare
    0,                         // tp_weaklistoffset
    0,                         // tp_iter
    0,                         // tp_iternext
    arkipy_Summary_methods, // tp_methods
    0,                         // tp_members
    0,                         // tp_getset
    0,                         // tp_base
    0,                         // tp_dict
    0,                         // tp_descr_get
    0,                         // tp_descr_set
    0,                         // tp_dictoffset
    (initproc)arkipy_Summary_init, // tp_init
    0,                         // tp_alloc
    0,                         // tp_new
};

}

namespace arki {
namespace python {

arkipy_Summary* summary_create()
{
    return (arkipy_Summary*)PyObject_CallObject((PyObject*)&arkipy_Summary_Type, NULL);
}

arkipy_Summary* summary_create(std::unique_ptr<Summary>&& summary)
{
    arkipy_Summary* result = PyObject_New(arkipy_Summary, &arkipy_Summary_Type);
    if (!result) return nullptr;
    result->summary = summary.release();
    return result;
}

void register_summary(PyObject* m)
{
    common_init();

    arkipy_Summary_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&arkipy_Summary_Type) < 0)
        return;
    Py_INCREF(&arkipy_Summary_Type);

    PyModule_AddObject(m, "Summary", (PyObject*)&arkipy_Summary_Type);
}

}
}


