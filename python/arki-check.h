#ifndef ARKI_PYTHON_ARKI_CHECK_H
#define ARKI_PYTHON_ARKI_CHECK_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
namespace runtime {
struct ArkiCheck;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::runtime::ArkiCheck* arki_check;
} arkipy_ArkiCheck;

extern PyTypeObject* arkipy_ArkiCheck_Type;

#define arkipy_ArkiCheck_Check(ob) \
    (Py_TYPE(ob) == &arkipy_ArkiCheck_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_ArkiCheck_Type))
}

namespace arki {
namespace python {

void register_arki_check(PyObject* m);

}
}

#endif
