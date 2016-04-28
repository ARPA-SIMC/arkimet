#include <Python.h>
#include "common.h"
#include "metadata.h"
#include "summary.h"
#include "dataset.h"
#include "arki/matcher.h"
#include "arki/configfile.h"
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

static PyMethodDef arkimet_methods[] = {
    { "expand_query", (PyCFunction)arkipy_expand_query, METH_VARARGS, "Return the same text query with all aliases expanded" },
    { "matcher_alias_database", (PyCFunction)arkipy_matcher_alias_database, METH_NOARGS, "Return a string with the current matcher alias database" },
    { NULL }
};

static PyModuleDef arkimet_module = {
    PyModuleDef_HEAD_INIT,
    "_arkimet",       /* m_name */
    "Arkimet Python interface.",  /* m_doc */
    -1,             /* m_size */
    arkimet_methods, /* m_methods */
    NULL,           /* m_reload */
    NULL,           /* m_traverse */
    NULL,           /* m_clear */
    NULL,           /* m_free */

};

PyMODINIT_FUNC PyInit__arkimet(void)
{
    using namespace arki::python;

    PyObject* m = PyModule_Create(&arkimet_module);

    register_metadata(m);
    register_summary(m);
    register_dataset(m);

    return m;
}

}
