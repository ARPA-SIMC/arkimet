#include <Python.h>
#include "summary.h"
#include "common.h"
#include "arki/summary.h"
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::python;

extern "C" {

/*
 * Summary
 */

static PyMethodDef arkipy_Summary_methods[] = {
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


