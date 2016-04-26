#include <Python.h>
#include "dataset.h"
#include "common.h"
#include "metadata.h"
#include "arki/dataset.h"
#include "arki/configfile.h"
#include "arki/sort.h"
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
static PyObject* arkipy_DatasetReader_copy(arkipy_DatasetReader* self)
{
    arkipy_DatasetReader* result = PyObject_New(arkipy_DatasetReader, &arkipy_DatasetReader_Type);
    if (!result) return NULL;
    try {
        result->rec = self->rec->clone().release();
        result->station_context = self->station_context;
        return (PyObject*)result;
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* arkipy_DatasetReader_clear(arkipy_DatasetReader* self)
{
    try {
        self->rec->clear();
        self->station_context = false;
        Py_RETURN_NONE;
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* arkipy_DatasetReader_clear_vars(arkipy_DatasetReader* self)
{
    try {
        self->rec->clear_vars();
        Py_RETURN_NONE;
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* arkipy_DatasetReader_var(arkipy_DatasetReader* self, PyObject* args)
{
    const char* name = NULL;
    if (!PyArg_ParseTuple(args, "s", &name))
        return nullptr;

    try {
        return (PyObject*)wrpy->var_create_copy((*self->rec)[name]);
    } DBALLE_CATCH_RETURN_PYO
}

static PyObject* arkipy_DatasetReader_key(arkipy_DatasetReader* self, PyObject* args)
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

static PyObject* arkipy_DatasetReader_update(arkipy_DatasetReader* self, PyObject *args, PyObject *kw)
{
    if (kw)
    {
        PyObject *key, *value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(kw, &pos, &key, &value))
            if (arkipy_DatasetReader_setitem(self, key, value) < 0)
                return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject* arkipy_DatasetReader_get(arkipy_DatasetReader* self, PyObject *args, PyObject* kw)
{
    static char* kwlist[] = { "key", "default", NULL };
    PyObject* key;
    PyObject* def = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O", kwlist, &key, &def))
        return nullptr;

    try {
        int has = arkipy_DatasetReader_contains(self, key);
        if (has < 0) return NULL;
        if (!has)
        {
            Py_INCREF(def);
            return def;
        }

        return arkipy_DatasetReader_getitem(self, key);
    } DBALLE_CATCH_RETURN_PYO
}


static PyObject* arkipy_DatasetReader_vars(arkipy_DatasetReader* self)
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

static PyObject* arkipy_DatasetReader_date_extremes(arkipy_DatasetReader* self)
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

static PyObject* arkipy_DatasetReader_set_station_context(arkipy_DatasetReader* self)
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

static PyObject* arkipy_DatasetReader_set_from_string(arkipy_DatasetReader* self, PyObject *args)
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


struct DataQuery
{
    /// Matcher used to select data
    Matcher matcher;

    /**
     * Hint for the dataset backend to let them know that we also want the data
     * and not just the metadata.
     *
     * This is currently only used by the HTTP client dataset, which will only
     * download data from the server if this option is set.
     */
    bool with_data;

    /// Optional compare function to define a custom ordering of the result
    std::shared_ptr<sort::Compare> sorter;

    DataQuery();
    DataQuery(const Matcher& matcher, bool with_data=false);
    ~DataQuery();

	void lua_from_table(lua_State* L, int idx);
	void lua_push_table(lua_State* L, int idx) const;
};

    /**
     * Query the dataset using the given matcher, and sending the results to
     * the given function
     */
    virtual void query_data(const dataset::DataQuery& q, metadata_dest_func dest) = 0;

    /**
     * Add to summary the summary of the data that would be extracted with the
     * given query.
     */
    virtual void query_summary(const Matcher& matcher, Summary& summary) = 0;
#endif

static PyObject* arkipy_DatasetReader_query_data(arkipy_DatasetReader* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "on_metadata", "matcher", "with_data", "sort", NULL };
    PyObject* arg_on_metadata = Py_None;
    PyObject* arg_matcher = Py_None;
    PyObject* arg_with_data = Py_None;
    PyObject* arg_sort = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|OOO", (char**)kwlist, &arg_on_metadata, &arg_matcher, &arg_with_data, &arg_sort))
        return nullptr;

    if (!PyCallable_Check(arg_on_metadata))
    {
        PyErr_SetString(PyExc_TypeError, "on_metadata must be a callable object");
        return nullptr;
    }
    string str_matcher;
    if (arg_matcher != Py_None && string_from_python(arg_matcher, str_matcher) == -1) return nullptr;
    bool with_data = false;
    if (arg_with_data != Py_None)
    {
        int istrue = PyObject_IsTrue(arg_with_data);
        if (istrue == -1) return nullptr;
        with_data = istrue == 1;
    }
    string sort;
    if (arg_sort != Py_None && string_from_python(arg_sort, sort) == -1) return nullptr;

    dataset::DataQuery query;
    query.matcher = Matcher::parse(str_matcher);
    query.with_data = with_data;
    if (!sort.empty()) query.sorter = sort::Compare::parse(sort);

    metadata_dest_func dest = [&](std::unique_ptr<Metadata>&& md) {
        // call arg_on_metadata
        pyo_unique_ptr args(PyTuple_Pack(1, metadata_create(move(md))));
        if (!args) throw python_callback_failed();
        pyo_unique_ptr res(PyObject_CallObject(arg_on_metadata, args));
        if (!res) throw python_callback_failed();
        // Continue if the callback returns None or True
        if (res == Py_None) return true;
        int cont = PyObject_IsTrue(res);
        if (cont == -1) throw python_callback_failed();
        return cont == 1;
    };

    try {
        self->ds->query_data(query, dest);
        Py_RETURN_NONE;
    } catch (python_callback_failed) {
        return nullptr;
    } ARKI_CATCH_RETURN_PYO
}

static PyObject* arkipy_DatasetReader_query_bytes(arkipy_DatasetReader* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "file", "matcher", "data_start_hook", "postprocess", "metadata_report", "summary_report", NULL };
    PyObject* arg_file = Py_None;
    PyObject* arg_matcher = Py_None;
    PyObject* arg_data_start_hook = Py_None;
    PyObject* arg_postprocess = Py_None;
    PyObject* arg_metadata_report = Py_None;
    PyObject* arg_summary_report = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|OOOOO", (char**)kwlist, &arg_file, &arg_matcher, &arg_data_start_hook, &arg_postprocess, &arg_metadata_report, &arg_summary_report))
        return nullptr;

    int fd = file_get_fileno(arg_file);
    if (fd == -1) return nullptr;
    string fd_name;
    if (object_repr(arg_file, fd_name) == -1) return nullptr;
    string str_matcher;
    if (arg_matcher != Py_None && string_from_python(arg_matcher, str_matcher) == -1) return nullptr;
    string postprocess;
    if (arg_postprocess != Py_None && string_from_python(arg_postprocess, postprocess) == -1) return nullptr;
    string metadata_report;
    if (arg_metadata_report != Py_None && string_from_python(arg_metadata_report, metadata_report) == -1) return nullptr;
    string summary_report;
    if (arg_summary_report != Py_None && string_from_python(arg_summary_report, summary_report) == -1) return nullptr;
    if (arg_data_start_hook != Py_None && !PyCallable_Check(arg_data_start_hook))
    {
        PyErr_SetString(PyExc_TypeError, "data_start_hoook must be None or a callable object");
        return nullptr;
    }

    Matcher matcher(Matcher::parse(str_matcher));

    dataset::ByteQuery query;
    if (!metadata_report.empty())
        query.setRepMetadata(matcher, metadata_report);
    else if (!summary_report.empty())
        query.setRepSummary(matcher, summary_report);
    else if (!postprocess.empty())
        query.setPostprocess(matcher, postprocess);
    else
        query.setData(matcher);

    pyo_unique_ptr data_start_hook_args;
    if (arg_data_start_hook != Py_None)
    {
        data_start_hook_args = pyo_unique_ptr(Py_BuildValue("()"));
        if (!data_start_hook_args) return nullptr;

        query.data_start_hook = [&](NamedFileDescriptor& fd) {
            // call arg_data_start_hook
            pyo_unique_ptr res(PyObject_CallObject(arg_data_start_hook, data_start_hook_args));
            if (!res) throw python_callback_failed();
        };
    }

    try {
        NamedFileDescriptor out(fd, fd_name);
        self->ds->query_bytes(query, out);
        Py_RETURN_NONE;
    } catch (python_callback_failed) {
        return nullptr;
    } ARKI_CATCH_RETURN_PYO
}

static PyMethodDef arkipy_DatasetReader_methods[] = {
    {"query_data", (PyCFunction)arkipy_DatasetReader_query_data, METH_VARARGS | METH_KEYWORDS, R"(
        query a dataset, processing the resulting metadata one by one.

        Arguments:
          on_metadata: a function called on each metadata, with the Metadata
                       object as its only argument. Return None or True to
                       continue processing results, False to stop.
          matcher: the matcher string to filter data to return.
          with_data: if True, also load data together with the metadata.
          sort: string with the desired sort order of results.
        )" },
    {"query_bytes", (PyCFunction)arkipy_DatasetReader_query_bytes, METH_VARARGS | METH_KEYWORDS, R"(
        query a dataset, piping results to a file.

        The file needs to be either an integer file or socket handle, or a
        file-like object with a fileno() method that returns an integer handle.

        Arguments:
          matcher: the matcher string to filter data to return
          data_start_hook: function called before sending the data to the file
          postprocess: name of a postprocessor to use to filter data server-side
          metadata_report: name of the server-side report function to run on results metadata
          summary_report: name of the server-side report function to run on results summary
        )" },
#if 0
    {"copy", (PyCFunction)arkipy_DatasetReader_copy, METH_NOARGS, "return a deep copy of the DatasetReader" },
    {"clear", (PyCFunction)arkipy_DatasetReader_clear, METH_NOARGS, "remove all data from the record" },
    {"clear_vars", (PyCFunction)arkipy_DatasetReader_clear_vars, METH_NOARGS, "remove all variables from the record, leaving the keywords intact" },
    {"keys", (PyCFunction)arkipy_DatasetReader_keys, METH_NOARGS, "return a list with all the keys set in the DatasetReader." },
    {"items", (PyCFunction)arkipy_DatasetReader_items, METH_NOARGS, "return a list with all the (key, value) tuples set in the DatasetReader." },
    {"varitems", (PyCFunction)arkipy_DatasetReader_varitems, METH_NOARGS, "return a list with all the (key, `dballe.Var`_) tuples set in the DatasetReader." },
    {"var", (PyCFunction)arkipy_DatasetReader_var, METH_VARARGS, "return a `dballe.Var`_ from the record, given its key." },
    {"update", (PyCFunction)arkipy_DatasetReader_update, METH_VARARGS | METH_KEYWORDS, "set many record keys/vars in a single shot, via kwargs" },
    {"get", (PyCFunction)arkipy_DatasetReader_get, METH_VARARGS | METH_KEYWORDS, "lookup a value, returning a fallback value (None by default) if unset" },

    // Deprecated
    {"key", (PyCFunction)arkipy_DatasetReader_var, METH_VARARGS, "(deprecated) return a `dballe.Var`_ from the record, given its key." },
    {"vars", (PyCFunction)arkipy_DatasetReader_vars, METH_NOARGS, "(deprecated) return a sequence with all the variables set on the DatasetReader. Note that this does not include keys." },
    {"date_extremes", (PyCFunction)arkipy_DatasetReader_date_extremes, METH_NOARGS, "(deprecated) get two datetime objects with the lower and upper bounds of the datetime period in this record" },
    {"set_station_context", (PyCFunction)arkipy_DatasetReader_set_station_context, METH_NOARGS, "(deprecated) set the date, level and time range values to match the station data context" },
    {"set_from_string", (PyCFunction)arkipy_DatasetReader_set_from_string, METH_VARARGS, "(deprecated) set values from a 'key=val' string" },
#endif
    {NULL}
};

static int arkipy_DatasetReader_init(arkipy_DatasetReader* self, PyObject* args, PyObject* kw)
{
    static const char* kwlist[] = { "cfg", nullptr };
    PyObject* cfg = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "O", (char**)kwlist, &cfg))
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

static void arkipy_DatasetReader_dealloc(arkipy_DatasetReader* self)
{
    delete self->ds;
    self->ds = nullptr;
}

static PyObject* arkipy_DatasetReader_str(arkipy_DatasetReader* self)
{
    return PyUnicode_FromFormat("DatasetReader(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
}

static PyObject* arkipy_DatasetReader_repr(arkipy_DatasetReader* self)
{
    return PyUnicode_FromFormat("DatasetReader(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
}

PyTypeObject arkipy_DatasetReader_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "arkimet.DatasetReader",   // tp_name
    sizeof(arkipy_DatasetReader), // tp_basicsize
    0,                         // tp_itemsize
    (destructor)arkipy_DatasetReader_dealloc, // tp_dealloc
    0,                         // tp_print
    0,                         // tp_getattr
    0,                         // tp_setattr
    0,                         // tp_compare
    (reprfunc)arkipy_DatasetReader_repr, // tp_repr
    0,                         // tp_as_number
    0,                         // tp_as_sequence
    0,                         // tp_as_mapping
    0,                         // tp_hash
    0,                         // tp_call
    (reprfunc)arkipy_DatasetReader_str,  // tp_str
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
    arkipy_DatasetReader_methods, // tp_methods
    0,                         // tp_members
    0,                         // tp_getset
    0,                         // tp_base
    0,                         // tp_dict
    0,                         // tp_descr_get
    0,                         // tp_descr_set
    0,                         // tp_dictoffset
    (initproc)arkipy_DatasetReader_init, // tp_init
    0,                         // tp_alloc
    0,                         // tp_new
};

}

namespace arki {
namespace python {

arkipy_DatasetReader* dataset_reader_create()
{
    return (arkipy_DatasetReader*)PyObject_CallObject((PyObject*)&arkipy_DatasetReader_Type, NULL);
}

void register_dataset(PyObject* m)
{
    common_init();

    arkipy_DatasetReader_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&arkipy_DatasetReader_Type) < 0)
        return;
    Py_INCREF(&arkipy_DatasetReader_Type);

    PyModule_AddObject(m, "DatasetReader", (PyObject*)&arkipy_DatasetReader_Type);
}

}
}
