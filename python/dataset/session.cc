#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "python/dataset/session.h"
#include "python/dataset.h"
#include "python/cfg.h"
#include "arki/dataset/session.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetSession_Type = nullptr;

}

namespace {

struct get_alias_database : public MethNoargs<get_alias_database, arkipy_DatasetSession>
{
    constexpr static const char* name = "get_alias_database";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "arkimet.cfg.Sections";
    constexpr static const char* summary = "return matcher alias database for this session";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            return cfg_sections(self->ptr->get_alias_database());
        } ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetSessionDef : public Type<DatasetSessionDef, arkipy_DatasetSession>
{
    constexpr static const char* name = "Session";
    constexpr static const char* qual_name = "arkimet.dataset.Session";
    constexpr static const char* doc = R"(
Shared configuration and data used to work with arkimet datasets.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<MethGenericEnter<Impl>, MethGenericExit<Impl>, get_alias_database> methods;

    static void _dealloc(Impl* self)
    {
        self->ptr.~shared_ptr<arki::dataset::Session>();
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
        static const char* kwlist[] = { "load_aliases", nullptr };
        int load_aliases = 1;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|p", const_cast<char**>(kwlist), &load_aliases))
            return -1;

        try {
            new (&(self->ptr)) shared_ptr<arki::dataset::Session>(std::make_shared<arki::dataset::Session>(load_aliases));
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetSessionDef* session_def = nullptr;

}


namespace arki {
namespace python {

arkipy_DatasetSession* dataset_session_create(std::shared_ptr<arki::dataset::Session> ptr)
{
    arkipy_DatasetSession* result = PyObject_New(arkipy_DatasetSession, arkipy_DatasetSession_Type);
    if (!result) return nullptr;
    new (&(result->ptr)) std::shared_ptr<arki::dataset::Session>(ptr);
    return result;
}

void register_dataset_session(PyObject* module)
{
    session_def = new DatasetSessionDef;
    session_def->define(arkipy_DatasetSession_Type, module);
}

}
}
