#ifndef ARKI_PYTHON_DATASET_H
#define ARKI_PYTHON_DATASET_H

#include <Python.h>
#include <memory>

namespace arki {
namespace dataset {
struct Reader;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::dataset::Reader* ds;
} arkipy_DatasetReader;

PyAPI_DATA(PyTypeObject) arkipy_DatasetReader_Type;

#define arkipy_DatasetReader_Check(ob) \
    (Py_TYPE(ob) == &arkipy_DatasetReader_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_DatasetReader_Type))
}

namespace arki {
namespace python {

arkipy_DatasetReader* dataset_reader_create();
arkipy_DatasetReader* dataset_reader_create(std::unique_ptr<dataset::Reader>&& ds);

void register_dataset(PyObject* m);

}
}
#endif
