#include <Python.h>
#include "dataset.h"
#include "common.h"
#include "metadata.h"
#include "summary.h"
#include "arki/core/file.h"
#include "arki/dataset.h"
#include "arki/configfile.h"
#include "arki/sort.h"
#include <vector>
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

/*
 * DatasetReader
 */

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

    try {
        dataset::DataQuery query;
        query.matcher = Matcher::parse(str_matcher);
        query.with_data = with_data;
        if (!sort.empty()) query.sorter = sort::Compare::parse(sort);

        metadata_dest_func dest = [&](std::unique_ptr<Metadata>&& md) {
            // call arg_on_metadata
            py_unique_ptr<arkipy_Metadata> pymd(metadata_create(move(md)));
            pyo_unique_ptr args(PyTuple_Pack(1, pymd.get()));
            if (!args) throw python_callback_failed();
            pyo_unique_ptr res(PyObject_CallObject(arg_on_metadata, args));
            if (!res) throw python_callback_failed();
            // Continue if the callback returns None or True
            if (res == Py_None) return true;
            int cont = PyObject_IsTrue(res);
            if (cont == -1) throw python_callback_failed();
            return cont == 1;
        };

        self->ds->query_data(query, dest);
        Py_RETURN_NONE;
    } ARKI_CATCH_RETURN_PYO
}

static PyObject* arkipy_DatasetReader_query_summary(arkipy_DatasetReader* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "matcher", "summary", NULL };
    PyObject* arg_matcher = Py_None;
    PyObject* arg_summary = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "|OO", (char**)kwlist, &arg_matcher, &arg_summary))
        return nullptr;

    string str_matcher;
    if (arg_matcher != Py_None && string_from_python(arg_matcher, str_matcher) == -1) return nullptr;

    Summary* summary = nullptr;
    if (arg_summary != Py_None)
    {
        if (!arkipy_Summary_Check(arg_summary))
        {
            PyErr_SetString(PyExc_TypeError, "summary must be None or an arkimet.Summary object");
            return nullptr;
        } else {
            summary = ((arkipy_Summary*)arg_summary)->summary;
        }
    }

    try {
        if (summary)
        {
            self->ds->query_summary(Matcher::parse(str_matcher), *summary);
            Py_INCREF(arg_summary);
            return (PyObject*)arg_summary;
        }
        else
        {
            py_unique_ptr<arkipy_Summary> res(summary_create());
            self->ds->query_summary(Matcher::parse(str_matcher), *res->summary);
            return (PyObject*)res.release();
        }
    } ARKI_CATCH_RETURN_PYO
}

static PyObject* arkipy_DatasetReader_query_bytes(arkipy_DatasetReader* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "file", "matcher", "with_data", "sort", "data_start_hook", "postprocess", "metadata_report", "summary_report", NULL };
    PyObject* arg_file = Py_None;
    PyObject* arg_matcher = Py_None;
    PyObject* arg_with_data = Py_None;
    PyObject* arg_sort = Py_None;
    PyObject* arg_data_start_hook = Py_None;
    PyObject* arg_postprocess = Py_None;
    PyObject* arg_metadata_report = Py_None;
    PyObject* arg_summary_report = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|OOOOOOO", (char**)kwlist, &arg_file, &arg_matcher, &arg_with_data, &arg_sort, &arg_data_start_hook, &arg_postprocess, &arg_metadata_report, &arg_summary_report))
        return nullptr;

    int fd = file_get_fileno(arg_file);
    if (fd == -1) return nullptr;
    string fd_name;
    if (object_repr(arg_file, fd_name) == -1) return nullptr;
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

    try {
        Matcher matcher(Matcher::parse(str_matcher));

        dataset::ByteQuery query;
        query.with_data = with_data;
        if (!sort.empty()) query.sorter = sort::Compare::parse(sort);
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

        NamedFileDescriptor out(fd, fd_name);
        self->ds->query_bytes(query, out);
        Py_RETURN_NONE;
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
    {"query_summary", (PyCFunction)arkipy_DatasetReader_query_summary, METH_VARARGS | METH_KEYWORDS, R"(
        query a dataset, returning an arkimet.Summary with the results.

        Arguments:
          matcher: the matcher string to filter data to return.
          summary: not None, add results to this arkimet.Summary, and return
                   it, instead of creating a new one.
        )" },
    {"query_bytes", (PyCFunction)arkipy_DatasetReader_query_bytes, METH_VARARGS | METH_KEYWORDS, R"(
        query a dataset, piping results to a file.

        Arguments:
          file: the output file. The file needs to be either an integer file or
                socket handle, or a file-like object with a fileno() method
                that returns an integer handle.
          matcher: the matcher string to filter data to return.
          with_data: if True, also load data together with the metadata.
          sort: string with the desired sort order of results.
          data_start_hook: function called before sending the data to the file
          postprocess: name of a postprocessor to use to filter data server-side
          metadata_report: name of the server-side report function to run on results metadata
          summary_report: name of the server-side report function to run on results summary
        )" },
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
        if (configfile_from_python(cfg, arki_cfg)) return -1;
        self->ds = dataset::Reader::create(arki_cfg).release();
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

arkipy_DatasetReader* dataset_reader_create(std::unique_ptr<dataset::Reader>&& ds)
{
    arkipy_DatasetReader* result = PyObject_New(arkipy_DatasetReader, &arkipy_DatasetReader_Type);
    if (!result) return nullptr;
    result->ds = ds.release();
    return result;
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
