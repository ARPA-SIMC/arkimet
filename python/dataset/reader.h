#ifndef ARKI_PYTHON_DATASET_READER_H
#define ARKI_PYTHON_DATASET_READER_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/dataset/fwd.h>
#include "python/utils/type.h"
#include "python/common.h"
#include <memory>

extern "C" {

typedef arki::python::SharedPtrWrapper<arki::dataset::Reader> arkipy_DatasetReader;
extern PyTypeObject* arkipy_DatasetReader_Type;

#define arkipy_DatasetReader_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetReader_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetReader_Type))

}

namespace arki {
namespace python {

arkipy_DatasetReader* dataset_reader_create();
arkipy_DatasetReader* dataset_reader_create(std::shared_ptr<arki::dataset::Reader> ds);

void register_dataset_reader(PyObject* module);

}
}
#endif

