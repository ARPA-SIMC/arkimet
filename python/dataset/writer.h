#ifndef ARKI_PYTHON_DATASET_WRITER_H
#define ARKI_PYTHON_DATASET_WRITER_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/dataset/fwd.h>
#include "python/utils/type.h"
#include "python/common.h"
#include <memory>

extern "C" {

typedef arki::python::SharedPtrWrapper<arki::dataset::Writer> arkipy_DatasetWriter;
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

void register_dataset_writer(PyObject* module);

}
}
#endif

