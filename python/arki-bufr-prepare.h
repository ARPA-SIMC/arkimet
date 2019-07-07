#ifndef ARKI_PYTHON_ARKI_BUFR_PREPARE_H
#define ARKI_PYTHON_ARKI_BUFR_PREPARE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
namespace runtime {
struct ArkiBUFRPrepare;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::runtime::ArkiBUFRPrepare* arki_bufr_prepare;
} arkipy_ArkiBUFRPrepare;

extern PyTypeObject* arkipy_ArkiBUFRPrepare_Type;

#define arkipy_ArkiBUFRPrepare_Check(ob) \
    (Py_TYPE(ob) == &arkipy_ArkiBUFRPrepare_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_ArkiBUFRPrepare_Type))
}

namespace arki {
namespace python {

void register_arki_bufr_prepare(PyObject* m);

}
}

#endif
