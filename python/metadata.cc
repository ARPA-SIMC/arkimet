#include <Python.h>
#include "metadata.h"
#include "common.h"
#include "arki/metadata.h"
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::python;

extern "C" {

/*
 * Metadata
 */
static PyObject* arkipy_Metadata_write(arkipy_Metadata* self, PyObject *args, PyObject* kw)
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
            NamedFileDescriptor out(fd, fd_name);
            self->md->write(out);
        } else if (strcmp(format, "yaml") == 0) {
            PyErr_SetString(PyExc_NotImplementedError, "serializing to YAML is not yet implemented");
            return nullptr;
        } else if (strcmp(format, "json") == 0) {
            PyErr_SetString(PyExc_NotImplementedError, "serializing to JSON is not yet implemented");
            return nullptr;
        } else {
            PyErr_Format(PyExc_ValueError, "Unsupported metadata serializati format: %s", format);
            return nullptr;
        }
        Py_RETURN_NONE;
    } catch (python_callback_failed) {
        return nullptr;
    } ARKI_CATCH_RETURN_PYO
}


static PyMethodDef arkipy_Metadata_methods[] = {
    {"write", (PyCFunction)arkipy_Metadata_write, METH_VARARGS | METH_KEYWORDS, R"(
        Write the metadata to a file.

        The file needs to be either an integer file or socket handle, or a
        file-like object with a fileno() method that returns an integer handle.

        Arguments:
          file: the output file. The file needs to be either an integer file or
                socket handle, or a file-like object with a fileno() method
                that returns an integer handle.
          format: "binary", "yaml", or "json". Default: "binary".
        )" },
    {NULL}
};

static int arkipy_Metadata_init(arkipy_Metadata* self, PyObject* args, PyObject* kw)
{
    // Metadata() should not be invoked as a constructor, and if someone does
    // this is better than a segfault later on
    PyErr_SetString(PyExc_NotImplementedError, "Cursor objects cannot be constructed explicitly");
    return -1;
}

static void arkipy_Metadata_dealloc(arkipy_Metadata* self)
{
    delete self->md;
    self->md = nullptr;
}

static PyObject* arkipy_Metadata_str(arkipy_Metadata* self)
{
    return PyUnicode_FromFormat("Metadata");
}

static PyObject* arkipy_Metadata_repr(arkipy_Metadata* self)
{
    return PyUnicode_FromFormat("Metadata");
}

PyTypeObject arkipy_Metadata_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "arkimet.Metadata",   // tp_name
    sizeof(arkipy_Metadata), // tp_basicsize
    0,                         // tp_itemsize
    (destructor)arkipy_Metadata_dealloc, // tp_dealloc
    0,                         // tp_print
    0,                         // tp_getattr
    0,                         // tp_setattr
    0,                         // tp_compare
    (reprfunc)arkipy_Metadata_repr, // tp_repr
    0,                         // tp_as_number
    0,                         // tp_as_sequence
    0,                         // tp_as_mapping
    0,                         // tp_hash
    0,                         // tp_call
    (reprfunc)arkipy_Metadata_str,  // tp_str
    0,                         // tp_getattro
    0,                         // tp_setattro
    0,                         // tp_as_buffer
    Py_TPFLAGS_DEFAULT,        // tp_flags
    R"(
        Arkimet metadata

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
    arkipy_Metadata_methods, // tp_methods
    0,                         // tp_members
    0,                         // tp_getset
    0,                         // tp_base
    0,                         // tp_dict
    0,                         // tp_descr_get
    0,                         // tp_descr_set
    0,                         // tp_dictoffset
    (initproc)arkipy_Metadata_init, // tp_init
    0,                         // tp_alloc
    0,                         // tp_new
};

}

namespace arki {
namespace python {

arkipy_Metadata* metadata_create(std::unique_ptr<Metadata>&& md)
{
    arkipy_Metadata* result = PyObject_New(arkipy_Metadata, &arkipy_Metadata_Type);
    if (!result) return nullptr;
    result->md = md.release();
    return result;
}

void register_metadata(PyObject* m)
{
    common_init();

    arkipy_Metadata_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&arkipy_Metadata_Type) < 0)
        return;
    Py_INCREF(&arkipy_Metadata_Type);

    PyModule_AddObject(m, "Metadata", (PyObject*)&arkipy_Metadata_Type);
}

}
}

