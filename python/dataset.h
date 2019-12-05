#ifndef ARKI_PYTHON_DATASET_H
#define ARKI_PYTHON_DATASET_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/dataset/fwd.h>
#include <memory>

extern "C" {

typedef struct {
    PyObject_HEAD
    std::shared_ptr<arki::dataset::Reader> ds;
} arkipy_DatasetReader;

extern PyTypeObject* arkipy_DatasetReader_Type;

#define arkipy_DatasetReader_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetReader_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetReader_Type))


typedef struct {
    PyObject_HEAD
    std::shared_ptr<arki::dataset::Writer> ds;
} arkipy_DatasetWriter;

extern PyTypeObject* arkipy_DatasetWriter_Type;

#define arkipy_DatasetWriter_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetWriter_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetWriter_Type))


extern PyObject* arkipy_ImportError;
extern PyObject* arkipy_ImportDuplicateError;
extern PyObject* arkipy_ImportFailedError;


typedef struct {
    PyObject_HEAD
    std::shared_ptr<arki::dataset::Checker> ds;
} arkipy_DatasetChecker;

extern PyTypeObject* arkipy_DatasetChecker_Type;

#define arkipy_DatasetChecker_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetChecker_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetChecker_Type))


typedef struct {
    PyObject_HEAD
    arki::dataset::SessionTimeOverride* o;
} arkipy_DatasetSessionTimeOverride;

extern PyTypeObject* arkipy_DatasetSessionTimeOverride_Type;

#define arkipy_DatasetSessionTimeOverride_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetSessionTimeOverride_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetSessionTimeOverride_Type))

}

namespace arki {
namespace python {

arkipy_DatasetReader* dataset_reader_create();
arkipy_DatasetReader* dataset_reader_create(std::shared_ptr<dataset::Reader> ds);

void register_dataset(PyObject* m);

}
}
#endif
