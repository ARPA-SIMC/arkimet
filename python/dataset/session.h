#ifndef ARKI_PYTHON_DATASET_SESSION_H
#define ARKI_PYTHON_DATASET_SESSION_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/dataset/fwd.h>
#include "python/utils/type.h"
#include "python/common.h"
#include <memory>

extern "C" {

typedef arki::python::SharedPtrWrapper<arki::dataset::Session> arkipy_DatasetSession;
extern PyTypeObject* arkipy_DatasetSession_Type;

#define arkipy_DatasetSession_Check(ob) \
    (Py_TYPE(ob) == arkipy_DatasetSession_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetSession_Type))

}

namespace arki {
namespace python {

arkipy_DatasetSession* dataset_session_create(std::shared_ptr<arki::dataset::Session> ptr);

void register_dataset_session(PyObject* module);

}
}
#endif

