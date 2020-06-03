#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "dataset.h"
#include "common.h"
#include "cfg.h"
#include "utils/values.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/dict.h"
#include "dataset/session.h"
#include "dataset/reader.h"
#include "dataset/writer.h"
#include "dataset/checker.h"
#include "arki/dataset/http.h"
#include "arki/dataset/time.h"
#include "arki/dataset/session.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetSessionTimeOverride_Type = nullptr;

}

namespace {

/*
 * dataset.SessionTimeOverride
 */

struct sto__exit__ : public MethKwargs<sto__exit__, arkipy_DatasetSessionTimeOverride>
{
    constexpr static const char* name = "__exit__";
    constexpr static const char* signature = "ext_type, ext_val, ext_tb";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "exc_type", "exc_val", "exc_tb", nullptr };
        PyObject* exc_type = nullptr;
        PyObject* exc_val = nullptr;
        PyObject* exc_tb = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OOO", const_cast<char**>(kwlist),
                &exc_type, &exc_val, &exc_tb))
            return nullptr;

        try {
            delete self->o;
            self->o = nullptr;
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetSessionTimeOverrideDef : public Type<DatasetSessionTimeOverrideDef, arkipy_DatasetSessionTimeOverride>
{
    constexpr static const char* name = "SessionTimeOverride";
    constexpr static const char* qual_name = "arkimet.dataset.SessionTimeOverride";
    constexpr static const char* doc = R"(
Write functions for an arkimet dataset.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<MethGenericEnter<Impl>, sto__exit__> methods;

    static void _dealloc(Impl* self)
    {
        delete self->o;
        self->o = nullptr;
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.SessionTimeOverride()");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.SessionTimeOverride()");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "time", nullptr };
        unsigned long long arg_time = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "K", const_cast<char**>(kwlist), &arg_time))
            return -1;

        try {
            self->o = new arki::dataset::SessionTimeOverride(arki::dataset::SessionTime::local_override(arg_time));
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetSessionTimeOverrideDef* session_time_override_def = nullptr;

/*
 * dataset module functions
 */

struct read_config : public MethKwargs<read_config, PyObject>
{
    constexpr static const char* name = "read_config";
    constexpr static const char* signature = "pathname: str";
    constexpr static const char* returns = "arki.cfg.Section";
    constexpr static const char* summary = "Read the configuration of a dataset at the given path or URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "pathname", nullptr };
        const char* pathname;
        Py_ssize_t pathname_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &pathname, &pathname_len))
            return nullptr;

        try {
            auto section = arki::dataset::Session::read_config(std::string(pathname, pathname_len));
            return cfg_section(std::move(section));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct read_configs : public MethKwargs<read_configs, PyObject>
{
    constexpr static const char* name = "read_configs";
    constexpr static const char* signature = "pathname: str";
    constexpr static const char* returns = "arki.cfg.Sections";
    constexpr static const char* summary = "Read the merged dataset configuration at the given path or URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "pathname", nullptr };
        const char* pathname;
        Py_ssize_t pathname_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &pathname, &pathname_len))
            return nullptr;

        try {
            auto sections = arki::dataset::Session::read_configs(std::string(pathname, pathname_len));
            return cfg_sections(std::move(sections));
        } ARKI_CATCH_RETURN_PYO
    }
};


Methods<read_config, read_configs> dataset_methods;


/*
 * dataset.http module functions
 */

struct load_cfg_sections : public MethKwargs<load_cfg_sections, PyObject>
{
    constexpr static const char* name = "load_cfg_sections";
    constexpr static const char* signature = "url: str";
    constexpr static const char* returns = "arki.cfg.Sections";
    constexpr static const char* summary = "Read the configuration of the datasets at the given URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "url", nullptr };
        const char* url;
        Py_ssize_t url_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &url, &url_len))
            return nullptr;

        try {
            auto sections = arki::dataset::http::Reader::load_cfg_sections(std::string(url, url_len));
            return cfg_sections(std::move(sections));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct get_alias_database : public MethKwargs<get_alias_database, PyObject>
{
    constexpr static const char* name = "get_alias_database";
    constexpr static const char* signature = "url: str";
    constexpr static const char* returns = "arki.cfg.Sections";
    constexpr static const char* summary = "Read the alias database for the server at the given URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "url", nullptr };
        const char* url;
        Py_ssize_t url_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &url, &url_len))
            return nullptr;

        try {
            auto sections = arki::dataset::http::Reader::getAliasDatabase(std::string(url, url_len));
            return cfg_sections(std::move(sections));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct expand_remote_query : public MethKwargs<expand_remote_query, PyObject>
{
    constexpr static const char* name = "expand_remote_query";
    constexpr static const char* signature = "remotes: arkimet.cfg.Sections, query: str";
    constexpr static const char* returns = "str";
    constexpr static const char* summary = "Expand aliases on the query for all remote datasets given.";
    constexpr static const char* doc = "An exception is raised if some remotes have conflicting aliases definition.";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "remotes", "query", nullptr };
        PyObject* remotes = nullptr;
        const char* query;
        Py_ssize_t query_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "Os#", const_cast<char**>(kwlist), &remotes, &query, &query_len))
            return nullptr;

        try {
            if (PyErr_WarnEx(PyExc_DeprecationWarning, "arkimet.dataset.http.expand_remote_query() will be replaced by something else, unfortunately not yet designed", 1))
                return nullptr;

            auto session = std::make_shared<arki::dataset::Session>();
            std::string expanded = session->expand_remote_query(
                    sections_from_python(remotes), std::string(query, query_len));
            return to_python(expanded);
        } ARKI_CATCH_RETURN_PYO
    }
};



Methods<load_cfg_sections, get_alias_database, expand_remote_query> http_methods;

}

extern "C" {

static PyModuleDef dataset_module = {
    PyModuleDef_HEAD_INIT,
    "dataset",         /* m_name */
    "Arkimet dataset C functions",  /* m_doc */
    -1,             /* m_size */
    dataset_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef http_module = {
    PyModuleDef_HEAD_INIT,
    "http",         /* m_name */
    "Arkimet http dataset C functions",  /* m_doc */
    -1,             /* m_size */
    http_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

}

namespace arki {
namespace python {

static thread_local std::shared_ptr<arki::dataset::Session> dataset_session;

std::shared_ptr<arki::dataset::Session> get_dataset_session()
{
    if (!dataset_session)
        dataset_session = make_shared<arki::dataset::Session>();
        // TODO: load aliases
    return dataset_session;
}

void register_dataset(PyObject* m)
{
    pyo_unique_ptr http = throw_ifnull(PyModule_Create(&http_module));

    pyo_unique_ptr dataset = throw_ifnull(PyModule_Create(&dataset_module));

    register_dataset_session(dataset);
    register_dataset_reader(dataset);
    register_dataset_writer(dataset);
    register_dataset_checker(dataset);

    session_time_override_def = new DatasetSessionTimeOverrideDef;
    session_time_override_def->define(arkipy_DatasetSessionTimeOverride_Type, dataset);

    if (PyModule_AddObject(dataset, "http", http.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(m, "dataset", dataset.release()) == -1)
        throw PythonException();
}

}
}
