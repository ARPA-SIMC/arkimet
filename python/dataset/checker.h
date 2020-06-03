#ifndef ARKI_PYTHON_DATASET_CHECKER_H
#define ARKI_PYTHON_DATASET_CHECKER_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/dataset/fwd.h>
#include "python/utils/type.h"
#include "python/common.h"
#include <memory>

extern "C" {

typedef arki::python::SharedPtrWrapper<arki::dataset::Checker> arkipy_DatasetChecker;
extern PyTypeObject* arkipy_DatasetChecker_Type;

#define arkipy_DatasetChecker_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetChecker_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetChecker_Type))

}

namespace arki {
namespace python {

void register_dataset_checker(PyObject* module);

}
}
#endif

