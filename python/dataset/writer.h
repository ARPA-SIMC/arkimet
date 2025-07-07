#ifndef ARKI_PYTHON_DATASET_WRITER_H
#define ARKI_PYTHON_DATASET_WRITER_H

#define PY_SSIZE_T_CLEAN
#include "python/common.h"
#include "python/utils/type.h"
#include <Python.h>
#include <arki/dataset/fwd.h>
#include <memory>

extern "C" {

typedef arki::python::SharedPtrWrapper<arki::dataset::Writer>
    arkipy_DatasetWriter;
extern PyTypeObject* arkipy_DatasetWriter_Type;

#define arkipy_DatasetWriter_Check(ob)                                         \
    (Py_TYPE(ob) == arkipy_DatasetWriter_Type ||                               \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetWriter_Type))

extern PyObject* arkipy_ImportError;
extern PyObject* arkipy_ImportDuplicateError;
extern PyObject* arkipy_ImportFailedError;
}

namespace arki {
namespace python {

arkipy_DatasetWriter*
dataset_writer_create(std::shared_ptr<arki::dataset::Writer> ds);

void register_dataset_writer(PyObject* module);

} // namespace python
} // namespace arki
#endif
