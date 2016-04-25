#include <Python.h>
#include "dataset.h"
#include "common.h"
#include "arki/dataset.h"
#include "arki/configfile.h"
#include <vector>
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::python;

extern "C" {

/*
 * DatasetReader
 */

#if 0
static PyObject* dpy_DatasetReader_copy(dpy_DatasetReader* self)
{
    dpy_DatasetReader* result = PyObject_New(dpy_DatasetReader, &dpy_DatasetReader_Type);
    if (!result) return NULL;
    try {
        result->rec = self->rec->clone().release();
        result->station_context = self->station_context;
        return (PyObject*)result;
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* dpy_DatasetReader_clear(dpy_DatasetReader* self)
{
    try {
        self->rec->clear();
        self->station_context = false;
        Py_RETURN_NONE;
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* dpy_DatasetReader_clear_vars(dpy_DatasetReader* self)
{
    try {
        self->rec->clear_vars();
        Py_RETURN_NONE;
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* dpy_DatasetReader_var(dpy_DatasetReader* self, PyObject* args)
{
    const char* name = NULL;
    if (!PyArg_ParseTuple(args, "s", &name))
        return nullptr;

    try {
        return (PyObject*)wrpy->var_create_copy((*self->rec)[name]);
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* dpy_DatasetReader_key(dpy_DatasetReader* self, PyObject* args)
{
    if (PyErr_WarnEx(PyExc_DeprecationWarning, "please use DatasetReader.var(name) instead of DatasetReader.key(name)", 1))
        return nullptr;
    const char* name = NULL;
    if (!PyArg_ParseTuple(args, "s", &name))
        return nullptr;

    try {
        return (PyObject*)wrpy->var_create_copy((*self->rec)[name]);
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* dpy_DatasetReader_update(dpy_DatasetReader* self, PyObject *args, PyObject *kw)
{
    if (kw)
    {
        PyObject *key, *value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(kw, &pos, &key, &value))
            if (dpy_DatasetReader_setitem(self, key, value) < 0)
                return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject* dpy_DatasetReader_get(dpy_DatasetReader* self, PyObject *args, PyObject* kw)
{
    static char* kwlist[] = { "key", "default", NULL };
    PyObject* key;
    PyObject* def = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O", kwlist, &key, &def))
        return nullptr;

    try {
        int has = dpy_DatasetReader_contains(self, key);
        if (has < 0) return NULL;
        if (!has)
        {
            Py_INCREF(def);
            return def;
        }

        return dpy_DatasetReader_getitem(self, key);
    } DBALLE_CATCH_RETURN_PYO
}


static PyObject* dpy_DatasetReader_vars(dpy_DatasetReader* self)
{
    if (PyErr_WarnEx(PyExc_DeprecationWarning, "DatasetReader.vars() may disappear in a future version of DB-All.e, and no replacement is planned", 1))
        return nullptr;

    const std::vector<wreport::Var*>& vars = core::DatasetReader::downcast(*self->rec).vars();

    pyo_unique_ptr result(PyTuple_New(vars.size()));
    if (!result) return NULL;

    try {
        for (size_t i = 0; i < vars.size(); ++i)
        {
            pyo_unique_ptr v((PyObject*)wrpy->var_create_copy(*vars[i]));
            if (!v) return nullptr;

            if (PyTuple_SetItem(result, i, v.release()))
                return nullptr;
        }
        return result.release();
    } catch (wreport::error& e) {
        return raise_wreport_exception(e);
    } catch (std::exception& se) {
        return raise_std_exception(se);
    }
}

static PyObject* dpy_DatasetReader_date_extremes(dpy_DatasetReader* self)
{
    if (PyErr_WarnEx(PyExc_DeprecationWarning, "DatasetReader.date_extremes may disappear in a future version of DB-All.e, and no replacement is planned", 1))
        return NULL;

    try {
        DatetimeRange dtr = core::DatasetReader::downcast(*self->rec).get_datetimerange();

        pyo_unique_ptr dt_min(datetime_to_python(dtr.min));
        pyo_unique_ptr dt_max(datetime_to_python(dtr.max));

        if (!dt_min || !dt_max) return NULL;

        return Py_BuildValue("(NN)", dt_min.release(), dt_max.release());
    } catch (wreport::error& e) {
        return raise_wreport_exception(e);
    } catch (std::exception& se) {
        return raise_std_exception(se);
    }
}

static PyObject* dpy_DatasetReader_set_station_context(dpy_DatasetReader* self)
{
    if (PyErr_WarnEx(PyExc_DeprecationWarning, "DatasetReader.set_station_context is deprecated in favour of using DB.query_station_data", 1))
        return NULL;
    try {
        self->rec->set_datetime(Datetime());
        self->rec->set_level(Level());
        self->rec->set_trange(Trange());
        self->station_context = true;
        Py_RETURN_NONE;
    } catch (wreport::error& e) {
        return raise_wreport_exception(e);
    } catch (std::exception& se) {
        return raise_std_exception(se);
    }
}

static PyObject* dpy_DatasetReader_set_from_string(dpy_DatasetReader* self, PyObject *args)
{
    if (PyErr_WarnEx(PyExc_DeprecationWarning, "DatasetReader.set_from_string() may disappear in a future version of DB-All.e, and no replacement is planned", 1))
        return nullptr;

    const char* str = NULL;
    if (!PyArg_ParseTuple(args, "s", &str))
        return NULL;

    try {
        core::DatasetReader::downcast(*self->rec).set_from_string(str);
        self->station_context = false;
        Py_RETURN_NONE;
    } catch (wreport::error& e) {
        return raise_wreport_exception(e);
    } catch (std::exception& se) {
        return raise_std_exception(se);
    }
}
#endif

static PyMethodDef dpy_DatasetReader_methods[] = {
#if 0
    {"copy", (PyCFunction)dpy_DatasetReader_copy, METH_NOARGS, "return a deep copy of the DatasetReader" },
    {"clear", (PyCFunction)dpy_DatasetReader_clear, METH_NOARGS, "remove all data from the record" },
    {"clear_vars", (PyCFunction)dpy_DatasetReader_clear_vars, METH_NOARGS, "remove all variables from the record, leaving the keywords intact" },
    {"keys", (PyCFunction)dpy_DatasetReader_keys, METH_NOARGS, "return a list with all the keys set in the DatasetReader." },
    {"items", (PyCFunction)dpy_DatasetReader_items, METH_NOARGS, "return a list with all the (key, value) tuples set in the DatasetReader." },
    {"varitems", (PyCFunction)dpy_DatasetReader_varitems, METH_NOARGS, "return a list with all the (key, `dballe.Var`_) tuples set in the DatasetReader." },
    {"var", (PyCFunction)dpy_DatasetReader_var, METH_VARARGS, "return a `dballe.Var`_ from the record, given its key." },
    {"update", (PyCFunction)dpy_DatasetReader_update, METH_VARARGS | METH_KEYWORDS, "set many record keys/vars in a single shot, via kwargs" },
    {"get", (PyCFunction)dpy_DatasetReader_get, METH_VARARGS | METH_KEYWORDS, "lookup a value, returning a fallback value (None by default) if unset" },

    // Deprecated
    {"key", (PyCFunction)dpy_DatasetReader_var, METH_VARARGS, "(deprecated) return a `dballe.Var`_ from the record, given its key." },
    {"vars", (PyCFunction)dpy_DatasetReader_vars, METH_NOARGS, "(deprecated) return a sequence with all the variables set on the DatasetReader. Note that this does not include keys." },
    {"date_extremes", (PyCFunction)dpy_DatasetReader_date_extremes, METH_NOARGS, "(deprecated) get two datetime objects with the lower and upper bounds of the datetime period in this record" },
    {"set_station_context", (PyCFunction)dpy_DatasetReader_set_station_context, METH_NOARGS, "(deprecated) set the date, level and time range values to match the station data context" },
    {"set_from_string", (PyCFunction)dpy_DatasetReader_set_from_string, METH_VARARGS, "(deprecated) set values from a 'key=val' string" },
#endif
    {NULL}
};

static int dpy_DatasetReader_init(dpy_DatasetReader* self, PyObject* args, PyObject* kw)
{
    static char* kwlist[] = { "cfg", nullptr };
    PyObject* cfg = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "O", kwlist, &cfg))
        return -1;

    try {
        ConfigFile arki_cfg;

        // Fill cfg with the contents of the cfg dict
        if (!PyDict_Check(cfg))
        {
            PyErr_SetString(PyExc_TypeError, "cfg argument must be an instance of a dict");
            return -1;
        }
        PyObject *key, *val;
        Py_ssize_t pos = 0;
        while (PyDict_Next(cfg, &pos, &key, &val))
        {
            string k, v;
            if (string_from_python(key, k)) return -1;
            if (string_from_python(val, v)) return -1;
            arki_cfg.setValue(k, v);
        }

        self->ds = dataset::Reader::create(arki_cfg);

        return 0;
    } ARKI_CATCH_RETURN_INT;
}

static void dpy_DatasetReader_dealloc(dpy_DatasetReader* self)
{
    delete self->ds;
    self->ds = nullptr;
}

static PyObject* dpy_DatasetReader_str(dpy_DatasetReader* self)
{
    return PyUnicode_FromFormat("DatasetReader(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
}

static PyObject* dpy_DatasetReader_repr(dpy_DatasetReader* self)
{
    return PyUnicode_FromFormat("DatasetReader(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
}

PyTypeObject dpy_DatasetReader_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "arkimet.DatasetReader",   // tp_name
    sizeof(dpy_DatasetReader), // tp_basicsize
    0,                         // tp_itemsize
    (destructor)dpy_DatasetReader_dealloc, // tp_dealloc
    0,                         // tp_print
    0,                         // tp_getattr
    0,                         // tp_setattr
    0,                         // tp_compare
    (reprfunc)dpy_DatasetReader_repr, // tp_repr
    0,                         // tp_as_number
    0,                         // tp_as_sequence
    0,                         // tp_as_mapping
    0,                         // tp_hash
    0,                         // tp_call
    (reprfunc)dpy_DatasetReader_str,  // tp_str
    0,                         // tp_getattro
    0,                         // tp_setattro
    0,                         // tp_as_buffer
    Py_TPFLAGS_DEFAULT,        // tp_flags
    R"(
        Read functions for an arkimet dataset.

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
    dpy_DatasetReader_methods, // tp_methods
    0,                         // tp_members
    0,                         // tp_getset
    0,                         // tp_base
    0,                         // tp_dict
    0,                         // tp_descr_get
    0,                         // tp_descr_set
    0,                         // tp_dictoffset
    (initproc)dpy_DatasetReader_init, // tp_init
    0,                         // tp_alloc
    0,                         // tp_new
};

}

namespace arki {
namespace python {

dpy_DatasetReader* dataset_reader_create()
{
    return (dpy_DatasetReader*)PyObject_CallObject((PyObject*)&dpy_DatasetReader_Type, NULL);
}

void register_dataset(PyObject* m)
{
    common_init();

    dpy_DatasetReader_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&dpy_DatasetReader_Type) < 0)
        return;
    Py_INCREF(&dpy_DatasetReader_Type);

    PyModule_AddObject(m, "DatasetReader", (PyObject*)&dpy_DatasetReader_Type);
}

}
}
