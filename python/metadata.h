#ifndef ARKI_PYTHON_METADATA_H
#define ARKI_PYTHON_METADATA_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
struct Metadata;
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::Metadata* md;
} arkipy_Metadata;

PyAPI_DATA(PyTypeObject) arkipy_Metadata_Type;

#define arkipy_Metadata_Check(ob) \
    (Py_TYPE(ob) == &arkipy_Metadata_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_Metadata_Type))
}

namespace arki {
namespace python {

arkipy_Metadata* metadata_create(std::unique_ptr<Metadata>&& md);

void register_metadata(PyObject* m);

}
}
#endif

