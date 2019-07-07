#ifndef ARKI_PYTHON_ARKI_XARGS_H
#define ARKI_PYTHON_ARKI_XARGS_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
namespace runtime {
struct ArkiXargs;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::runtime::ArkiXargs* arki_xargs;
} arkipy_ArkiXargs;

extern PyTypeObject* arkipy_ArkiXargs_Type;

#define arkipy_ArkiXargs_Check(ob) \
    (Py_TYPE(ob) == &arkipy_ArkiXargs_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_ArkiXargs_Type))
}

namespace arki {
namespace python {

void register_arki_xargs(PyObject* m);

}
}

#endif
