#define PY_SSIZE_T_CLEAN
#include "python/dataset/session.h"
#include "arki/dataset.h"
#include "arki/dataset/pool.h"
#include "arki/dataset/session.h"
#include "python/cfg.h"
#include "python/dataset.h"
#include "python/dataset/checker.h"
#include "python/dataset/dataset.h"
#include "python/dataset/reader.h"
#include "python/dataset/writer.h"
#include "python/matcher.h"
#include <Python.h>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetSession_Type = nullptr;
}

namespace {

struct get_alias_database
    : public MethNoargs<get_alias_database, arkipy_DatasetSession>
{
    constexpr static const char* name    = "get_alias_database";
    constexpr static const char* returns = "arkimet.cfg.Sections";
    constexpr static const char* summary =
        "return matcher alias database for this session";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try
        {
            return to_python(self->session->get_alias_database());
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct datasets : public MethNoargs<datasets, arkipy_DatasetSession>
{
    constexpr static const char* name    = "datasets";
    constexpr static const char* returns = "List[arkimet.dataset.Dataset]";
    constexpr static const char* summary =
        "return a list of all datasets in the session pool";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try
        {
            pyo_unique_ptr res(throw_ifnull(PyList_New(0)));
            PyObject* list = res.get();
            self->pool->foreach_dataset(
                [&](std::shared_ptr<arki::dataset::Dataset> dataset) {
                    pyo_unique_ptr ds(
                        (PyObject*)dataset_dataset_create(dataset));
                    if (PyList_Append(list, ds))
                        throw PythonException();
                    return true;
                });
            return res.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct has_datasets : public MethNoargs<has_datasets, arkipy_DatasetSession>
{
    constexpr static const char* name    = "has_datasets";
    constexpr static const char* returns = "bool";
    constexpr static const char* summary =
        "return True if the session contains datasets in the dataset pool";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try
        {
            if (self->pool->has_datasets())
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct dataset_pool_size
    : public MethNoargs<dataset_pool_size, arkipy_DatasetSession>
{
    constexpr static const char* name    = "dataset_pool_size";
    constexpr static const char* returns = "int";
    constexpr static const char* summary =
        "return how many datasets are in the dataset pool";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try
        {
            return to_python(self->pool->size());
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct has_dataset : public MethKwargs<has_dataset, arkipy_DatasetSession>
{
    constexpr static const char* name      = "has_dataset";
    constexpr static const char* signature = "name: str";
    constexpr static const char* returns   = "bool";
    constexpr static const char* summary =
        "check if the dataset pool has a dataset with the given name";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"name", nullptr};
        const char* query           = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s",
                                         const_cast<char**>(kwlist), &query))
            return nullptr;

        try
        {
            if (self->pool->has_dataset(query))
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct matcher : public MethKwargs<matcher, arkipy_DatasetSession>
{
    constexpr static const char* name      = "matcher";
    constexpr static const char* signature = "query: str";
    constexpr static const char* returns   = "arkimet.Matcher";
    constexpr static const char* summary =
        "parse an arkimet matcher expression, using the aliases in this "
        "session, and return the Matcher object for it";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"query", nullptr};
        const char* query           = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s",
                                         const_cast<char**>(kwlist), &query))
            return nullptr;

        try
        {
            return matcher_to_python(self->session->matcher(query));
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct expand_query : public MethKwargs<expand_query, arkipy_DatasetSession>
{
    constexpr static const char* name      = "expand_query";
    constexpr static const char* signature = "query: str";
    constexpr static const char* returns   = "str";
    constexpr static const char* summary =
        "expand aliases in an Arkimet query, returning the same query without "
        "use of aliases";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"query", nullptr};
        const char* query           = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s",
                                         const_cast<char**>(kwlist), &query))
            return nullptr;

        try
        {
            Matcher m = self->session->matcher(query);
            return to_python(m.toStringExpanded());
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct load_aliases : public MethKwargs<load_aliases, arkipy_DatasetSession>
{
    constexpr static const char* name = "load_aliases";
    constexpr static const char* signature =
        "aliases: Union[str, arkimet.cfg.Sections]";
    constexpr static const char* summary =
        "add the given set of aliases to the alias database in this session";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"aliases", nullptr};
        PyObject* arg_aliases       = nullptr;
        if (!PyArg_ParseTupleAndKeywords(
                args, kw, "O", const_cast<char**>(kwlist), &arg_aliases))
            return nullptr;

        try
        {
            self->session->load_aliases(*sections_from_python(arg_aliases));
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct add_dataset : public MethKwargs<add_dataset, arkipy_DatasetSession>
{
    constexpr static const char* name = "add_dataset";
    constexpr static const char* signature =
        "cfg: Union[str, arkimet.cfg.Section, Dict[str, str]]";
    constexpr static const char* summary = "add a dataset to the Session pool";
    constexpr static const char* doc     = R"(
If a dataset with the same name already exists in the pool, it raises an
exception.

If the dataset is remote, the aliases used by the remote server will be
added to the session alias database. If different servers define some
aliases differently, it raises an exception.
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"cfg", nullptr};
        PyObject* arg_cfg           = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O",
                                         const_cast<char**>(kwlist), &arg_cfg))
            return nullptr;

        try
        {
            self->pool->add_dataset(*section_from_python(arg_cfg));
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

template <typename Base, typename Impl>
struct dataset_accessor_factory : public MethKwargs<Base, Impl>
{
    constexpr static const char* signature =
        "cfg: Union[str, arkimet.cfg.Section, Dict[str, str]] = None, name: "
        "str = None";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"cfg", "name", nullptr};
        PyObject* arg_cfg           = nullptr;
        const char* arg_name        = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|$Os",
                                         const_cast<char**>(kwlist), &arg_cfg,
                                         &arg_name))
            return nullptr;

        try
        {
            std::shared_ptr<arki::dataset::Dataset> ds;
            if (arg_cfg)
            {
                if (arg_name)
                {
                    PyErr_SetString(PyExc_ValueError,
                                    "only one of cfg or name must be passed");
                    throw PythonException();
                }
                ds = self->session->dataset(*section_from_python(arg_cfg));
            }
            else if (arg_name)
            {
                ds = self->pool->dataset(arg_name);
            }
            else
            {
                // Error
                PyErr_SetString(PyExc_ValueError,
                                "one of cfg or name must be passed");
                throw PythonException();
            }
            return Base::create_accessor(ds);
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct dataset : public dataset_accessor_factory<dataset, arkipy_DatasetSession>
{
    constexpr static const char* name    = "dataset";
    constexpr static const char* returns = "arkimet.dataset.Dataset";
    constexpr static const char* summary =
        "return a Dataset give its configuration";
    constexpr static const char* doc = nullptr;

    static PyObject*
    create_accessor(std::shared_ptr<arki::dataset::Dataset> dataset)
    {
        return (PyObject*)dataset_dataset_create(dataset);
    }
};

struct dataset_reader
    : public dataset_accessor_factory<dataset_reader, arkipy_DatasetSession>
{
    constexpr static const char* name    = "dataset_reader";
    constexpr static const char* returns = "arkimet.dataset.Reader";
    constexpr static const char* summary =
        "return a dataset reader give its configuration";
    constexpr static const char* doc = nullptr;

    static PyObject*
    create_accessor(std::shared_ptr<arki::dataset::Dataset> dataset)
    {
        return (PyObject*)dataset_reader_create(dataset->create_reader());
    }
};

struct dataset_writer
    : public dataset_accessor_factory<dataset_writer, arkipy_DatasetSession>
{
    constexpr static const char* name    = "dataset_writer";
    constexpr static const char* returns = "arkimet.dataset.Writer";
    constexpr static const char* summary =
        "return a dataset writer give its configuration";
    constexpr static const char* doc = nullptr;

    static PyObject*
    create_accessor(std::shared_ptr<arki::dataset::Dataset> dataset)
    {
        return (PyObject*)dataset_writer_create(dataset->create_writer());
    }
};

struct dataset_checker
    : public dataset_accessor_factory<dataset_checker, arkipy_DatasetSession>
{
    constexpr static const char* name    = "dataset_checker";
    constexpr static const char* returns = "arkimet.dataset.Checker";
    constexpr static const char* summary =
        "return a dataset checker give its configuration";
    constexpr static const char* doc = nullptr;

    static PyObject*
    create_accessor(std::shared_ptr<arki::dataset::Dataset> dataset)
    {
        return (PyObject*)dataset_checker_create(dataset->create_checker());
    }
};

struct querymacro : public MethKwargs<querymacro, arkipy_DatasetSession>
{
    constexpr static const char* name      = "querymacro";
    constexpr static const char* signature = "name: str, macro: str";
    constexpr static const char* returns   = "arkimet.dataset.Dataset";
    constexpr static const char* summary =
        "create a QueryMacro dataset querying datasets from this session's "
        "pool";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"name", "macro", nullptr};
        const char* name            = nullptr;
        const char* macro           = nullptr;
        if (!PyArg_ParseTupleAndKeywords(
                args, kw, "ss", const_cast<char**>(kwlist), &name, &macro))
            return nullptr;

        try
        {
            return (PyObject*)dataset_dataset_create(
                self->pool->querymacro(name, macro));
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct merged : public MethNoargs<merged, arkipy_DatasetSession>
{
    constexpr static const char* name    = "merged";
    constexpr static const char* returns = "arkimet.dataset.Dataset";
    constexpr static const char* summary =
        "return a merged dataset querying all datasets in this session";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try
        {
            return (PyObject*)dataset_dataset_create(self->pool->merged());
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetSessionDef : public Type<DatasetSessionDef, arkipy_DatasetSession>
{
    constexpr static const char* name      = "Session";
    constexpr static const char* qual_name = "arkimet.dataset.Session";
    constexpr static const char* doc       = R"(
Shared configuration, aliases, and working data used to work with arkimet datasets.

A Session stores alias information, and preloads :class:`arkimet.dataset.Dataset` objects.

Adding a remote dataset to the Session dataset pool will download the alias
database of its server and merge it into the current one, raising an error in
case of inconsistencies.

Datasets in the pool can be referred by name. It is possible to create datasets
given their configuration, without adding them to the pool.

Session is also used to instantiate matchers using its alias database.

Examples::

    # Long version
    with arkimet.dataset.Session() as session:
        for section in config.values():
            session.add_dataset(section)
        matcher = session.matcher("level:ground")
        with session.dataset("dsname") as dataset:
            with dataset.reader() as reader:
                result = reader.query_data(matcher)

    # Short version
    with arkimet.dataset.Session() as session:
        for section in config.values():
            session.add_dataset(section)
        with session.dataset_reader("dsname") as reader:
            with dataset.reader() as reader:
                result = reader.query_data("level:ground")
)";
    GetSetters<> getsetters;
    Methods<MethGenericEnter<Impl>, MethGenericExit<Impl>, get_alias_database,
            matcher, expand_query, load_aliases, datasets, has_datasets,
            has_dataset, dataset_pool_size, add_dataset, dataset,
            dataset_reader, dataset_writer, dataset_checker, querymacro, merged>
        methods;

    static void _dealloc(Impl* self)
    {
        self->pool.~shared_ptr<arki::dataset::Pool>();
        self->session.~shared_ptr<arki::dataset::Session>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Session");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Session");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"load_aliases", "force_dir_segments",
                                       nullptr};
        int load_aliases            = 1;
        int force_dir_segments      = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|pp",
                                         const_cast<char**>(kwlist),
                                         &load_aliases, &force_dir_segments))
            return -1;

        try
        {
            if (force_dir_segments)
                new (&(self->session)) shared_ptr<arki::dataset::Session>(
                    std::make_shared<arki::dataset::DirSegmentsSession>(
                        load_aliases));
            else
                new (&(self->session)) shared_ptr<arki::dataset::Session>(
                    std::make_shared<arki::dataset::Session>(load_aliases));
            new (&(self->pool)) shared_ptr<arki::dataset::Pool>(
                std::make_shared<arki::dataset::Pool>(self->session));
            return 0;
        }
        ARKI_CATCH_RETURN_INT;
    }
};

DatasetSessionDef* session_def = nullptr;

} // namespace

namespace arki {
namespace python {

arkipy_DatasetSession*
dataset_session_create(std::shared_ptr<arki::dataset::Session> session,
                       std::shared_ptr<arki::dataset::Pool> pool)
{
    arkipy_DatasetSession* result =
        PyObject_New(arkipy_DatasetSession, arkipy_DatasetSession_Type);
    if (!result)
        return nullptr;
    new (&(result->session)) std::shared_ptr<arki::dataset::Session>(session);
    new (&(result->pool)) std::shared_ptr<arki::dataset::Pool>(pool);
    return result;
}

std::shared_ptr<arki::dataset::Session> session_from_python(PyObject* o)
{
    try
    {
        if (arkipy_DatasetSession_Check(o))
        {
            return ((arkipy_DatasetSession*)o)->session;
        }
        PyErr_SetString(PyExc_TypeError,
                        "value must be an instance of arkimet.dataset.Session");
        throw PythonException();
    }
    ARKI_CATCH_RETHROW_PYTHON
}

std::shared_ptr<arki::dataset::Pool> pool_from_python(PyObject* o)
{
    try
    {
        if (arkipy_DatasetSession_Check(o))
        {
            return ((arkipy_DatasetSession*)o)->pool;
        }
        PyErr_SetString(PyExc_TypeError,
                        "value must be an instance of arkimet.dataset.Session");
        throw PythonException();
    }
    ARKI_CATCH_RETHROW_PYTHON
}

void register_dataset_session(PyObject* module)
{
    session_def = new DatasetSessionDef;
    session_def->define(arkipy_DatasetSession_Type, module);
}

} // namespace python
} // namespace arki
