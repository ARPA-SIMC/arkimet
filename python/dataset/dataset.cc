#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "python/dataset/dataset.h"
#include "python/dataset.h"
#include "python/dataset/reader.h"
#include "python/dataset/writer.h"
#include "python/dataset/checker.h"
#include "python/cfg.h"
#include "python/matcher.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetDataset_Type = nullptr;

}

namespace {

struct reader : public MethNoargs<reader, arkipy_DatasetDataset>
{
    constexpr static const char* name = "dataset_reader";
    constexpr static const char* returns = "arkimet.dataset.Reader";
    constexpr static const char* summary = "return a reader for this dataset";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            return (PyObject*)dataset_reader_create(self->ptr->create_reader());
        } ARKI_CATCH_RETURN_PYO
    }
};

struct writer : public MethNoargs<writer, arkipy_DatasetDataset>
{
    constexpr static const char* name = "dataset_writer";
    constexpr static const char* returns = "arkimet.dataset.Writer";
    constexpr static const char* summary = "return a writer for this dataset";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            return (PyObject*)dataset_writer_create(self->ptr->create_writer());
        } ARKI_CATCH_RETURN_PYO
    }
};

struct checker : public MethNoargs<checker, arkipy_DatasetDataset>
{
    constexpr static const char* name = "dataset_checker";
    constexpr static const char* returns = "arkimet.dataset.Checker";
    constexpr static const char* summary = "return a checker for this dataset";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            return (PyObject*)dataset_checker_create(self->ptr->create_checker());
        } ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetDatasetDef : public Type<DatasetDatasetDef, arkipy_DatasetDataset>
{
    constexpr static const char* name = "Dataset";
    constexpr static const char* qual_name = "arkimet.dataset.Dataset";
    constexpr static const char* doc = R"(
A dataset in arkimet. It provides information about the dataset configuration,
and allows to create readers, writers, and checkers to work with the dataset.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<MethGenericEnter<Impl>, MethGenericExit<Impl>, reader, writer, checker> methods;

    static void _dealloc(Impl* self)
    {
        self->ptr.~shared_ptr<arki::dataset::Dataset>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Dataset(%s)", self->ptr->name().c_str());
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Dataset(%s)", self->ptr->name().c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        PyErr_SetString(PyExc_NotImplementedError, "arki.dataset.Session.dataset() is currently the only supported way to istantiate Dataset objects");
        return -1;
    }
};

DatasetDatasetDef* dataset_def = nullptr;

}


namespace arki {
namespace python {

arkipy_DatasetDataset* dataset_dataset_create(std::shared_ptr<arki::dataset::Dataset> ptr)
{
    arkipy_DatasetDataset* result = PyObject_New(arkipy_DatasetDataset, arkipy_DatasetDataset_Type);
    if (!result) return nullptr;
    new (&(result->ptr)) std::shared_ptr<arki::dataset::Dataset>(ptr);
    return result;
}

std::shared_ptr<arki::dataset::Dataset> dataset_from_python(PyObject* o)
{
    try {
        if (arkipy_DatasetDataset_Check(o)) {
            return ((arkipy_DatasetDataset*)o)->ptr;
        }
        PyErr_SetString(PyExc_TypeError, "value must be an instance of arkimet.dataset.Dataset");
        throw PythonException();
    } ARKI_CATCH_RETHROW_PYTHON
}

void register_dataset_dataset(PyObject* module)
{
    dataset_def = new DatasetDatasetDef;
    dataset_def->define(arkipy_DatasetDataset_Type, module);
}

}
}
