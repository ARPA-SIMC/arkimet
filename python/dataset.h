#ifndef ARKI_PYTHON_DATASET_H
#define ARKI_PYTHON_DATASET_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/dataset/fwd.h>
#include "utils/type.h"
#include "common.h"
#include <memory>

extern "C" {

struct arkipy_DatasetSessionTimeOverride
{
    PyObject_HEAD
    arki::dataset::SessionTimeOverride* o;
};

extern PyTypeObject* arkipy_DatasetSessionTimeOverride_Type;

#define arkipy_DatasetSessionTimeOverride_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetSessionTimeOverride_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetSessionTimeOverride_Type))

}

namespace arki {
namespace python {

void register_dataset(PyObject* m);

}
}
#endif
