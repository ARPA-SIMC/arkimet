#ifndef ARKI_PYTHON_DATASET_H
#define ARKI_PYTHON_DATASET_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
namespace dataset {
struct Reader;
struct Writer;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::dataset::Reader* ds;
} arkipy_DatasetReader;

extern PyTypeObject* arkipy_DatasetReader_Type;

#define arkipy_DatasetReader_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetReader_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetReader_Type))


typedef struct {
    PyObject_HEAD
    arki::dataset::Writer* ds;
} arkipy_DatasetWriter;

extern PyTypeObject* arkipy_DatasetWriter_Type;

#define arkipy_DatasetWriter_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetWriter_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetWriter_Type))


extern PyObject* arkipy_ImportError;
extern PyObject* arkipy_ImportDuplicateError;
extern PyObject* arkipy_ImportFailedError;

}

namespace arki {
namespace python {

arkipy_DatasetReader* dataset_reader_create();
arkipy_DatasetReader* dataset_reader_create(std::unique_ptr<dataset::Reader>&& ds);

void register_dataset(PyObject* m);

}
}
#endif
