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


struct DatasetSessionDef : public Type<DatasetSessionDef, arkipy_DatasetSession>
{
    constexpr static const char* name = "Session";
    constexpr static const char* qual_name = "arkimet.dataset.Session";
    constexpr static const char* doc = R"(
Read functions for an arkimet dataset.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<MethGenericEnter<Impl>, MethGenericExit<Impl>> methods;

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
        static const char* kwlist[] = { "cfg", nullptr };
        PyObject* py_cfg = Py_None;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_cfg))
            return -1;

        try {
            core::cfg::Section cfg;

            if (PyUnicode_Check(py_cfg))
                cfg = arki::dataset::Session::read_config(from_python<std::string>(py_cfg));
            else
                cfg = section_from_python(py_cfg);

            new (&(self->ptr)) shared_ptr<arki::dataset::Session>(std::make_shared<arki::dataset::Session>());
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetSessionDef* reader_def = nullptr;

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
    reader_def = new DatasetSessionDef;
    reader_def->define(arkipy_DatasetSession_Type, module);
}

}
}

