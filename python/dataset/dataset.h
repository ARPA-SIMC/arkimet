#ifndef ARKI_PYTHON_DATASET_DATASET_H
#define ARKI_PYTHON_DATASET_DATASET_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/dataset/fwd.h>
#include "python/utils/type.h"
#include "python/common.h"
#include <memory>

extern "C" {

typedef arki::python::SharedPtrWrapper<arki::dataset::Dataset> arkipy_DatasetDataset;
extern PyTypeObject* arkipy_DatasetDataset_Type;

#define arkipy_DatasetDataset_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetDataset_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetDataset_Type))

}

namespace arki {
namespace python {

arkipy_DatasetDataset* dataset_dataset_create(std::shared_ptr<arki::dataset::Dataset> ptr);

/**
 * Return a arki::dataset::Dataset from a Python object that contains it
 */
std::shared_ptr<arki::dataset::Dataset> dataset_from_python(PyObject* o);

void register_dataset_dataset(PyObject* module);

}
}
#endif

