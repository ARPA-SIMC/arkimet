#include <Python.h>
#include "common.h"
#include "metadata.h"
#include "summary.h"
#include "dataset.h"
#include "arki_query.h"
#include "arki/matcher.h"
#include "arki/configfile.h"
#include "arki/runtime.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/http.h"
#include "arki/querymacro.h"
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::python;

extern "C" {

static PyObject* arkipy_expand_query(PyTypeObject *type, PyObject *args)
{
    const char* query;
    if (!PyArg_ParseTuple(args, "s", &query))
        return nullptr;
    try {
        Matcher m = Matcher::parse(query);
        string out = m.toStringExpanded();
        return PyUnicode_FromStringAndSize(out.data(), out.size());
    } ARKI_CATCH_RETURN_PYO
}

static PyObject* arkipy_matcher_alias_database(PyTypeObject *type, PyObject *none)
{
    try {
        ConfigFile cfg;
        MatcherAliasDatabase::serialise(cfg);
        string out = cfg.serialize();
        return PyUnicode_FromStringAndSize(out.data(), out.size());
    } ARKI_CATCH_RETURN_PYO
}

static PyObject* arkipy_make_qmacro_dataset(arkipy_Metadata* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "cfg", "datasets", "name", "query", NULL };
    PyObject* arg_cfg = Py_None;
    PyObject* arg_datasets = Py_None;
    const char* name = nullptr;
    const char* query = "";

    if (!PyArg_ParseTupleAndKeywords(args, kw, "OOs|s", (char**)kwlist, &arg_cfg, &arg_datasets, &name, &query))
        return nullptr;

    ConfigFile cfg;
    if (configfile_from_python(arg_cfg, cfg)) return nullptr;
    ConfigFile datasets;
    if (configfile_from_python(arg_datasets, datasets)) return nullptr;

    try {
        unique_ptr<dataset::Reader> ds;
        string baseurl = dataset::http::Reader::allSameRemoteServer(datasets);
        if (baseurl.empty())
        {
            // Create the local query macro
            ds.reset(new Querymacro(cfg, datasets, name, query));
        } else {
            // Create the remote query macro
            ConfigFile cfg;
            cfg.setValue("name", name);
            cfg.setValue("type", "remote");
            cfg.setValue("path", baseurl);
            cfg.setValue("qmacro", query);
            ds = dataset::Reader::create(cfg);
        }

        return (PyObject*)dataset_reader_create(move(ds));
    } ARKI_CATCH_RETURN_PYO
}

static PyObject* arkipy_make_merged_dataset(arkipy_Metadata* self, PyObject *args, PyObject* kw)
{
    static const char* kwlist[] = { "cfg", NULL };
    PyObject* arg_cfg = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "O", (char**)kwlist, &arg_cfg))
        return nullptr;

    ConfigFile cfg;
    if (configfile_from_python(arg_cfg, cfg)) return nullptr;

    try {
        std::unique_ptr<dataset::Merged> ds(new dataset::Merged);
        for (auto i = cfg.sectionBegin(); i != cfg.sectionEnd(); ++i)
            ds->add_dataset(*i->second);
        return (PyObject*)dataset_reader_create(move(ds));
    } ARKI_CATCH_RETURN_PYO
}

static PyMethodDef arkimet_methods[] = {
    { "expand_query", (PyCFunction)arkipy_expand_query, METH_VARARGS, "Return the same text query with all aliases expanded" },
    { "matcher_alias_database", (PyCFunction)arkipy_matcher_alias_database, METH_NOARGS, "Return a string with the current matcher alias database" },
    { "make_merged_dataset", (PyCFunction)arkipy_make_merged_dataset, METH_VARARGS | METH_KEYWORDS, R"(
        Create a merged dataset from all the datasets found in the given configuration
        )" },
    { "make_qmacro_dataset", (PyCFunction)arkipy_make_qmacro_dataset, METH_VARARGS | METH_KEYWORDS, R"(
        Create a qmacro dataset that aggregates the contents of multiple datasets.

        Arguments:
          cfg: base configuration of the resulting dataset
          datasets: configuration of all the available datasets
          name: name of the query macro to use
        )" },
    { NULL, NULL, 0, NULL }
};

static PyModuleDef arkimet_module = {
    PyModuleDef_HEAD_INIT,
    "_arkimet",       /* m_name */
    "Arkimet Python interface.",  /* m_doc */
    -1,             /* m_size */
    arkimet_methods, /* m_methods */
    NULL,           /* m_slots */
    NULL,           /* m_traverse */
    NULL,           /* m_clear */
    NULL,           /* m_free */

};

PyMODINIT_FUNC PyInit__arkimet(void)
{
    using namespace arki::python;

    PyObject* m = PyModule_Create(&arkimet_module);
    if (!m) return m;

    register_metadata(m);
    register_summary(m);
    register_dataset(m);
    register_arki_query(m);

    return m;
}

}
