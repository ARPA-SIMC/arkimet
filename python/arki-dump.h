#ifndef ARKI_PYTHON_ARKI_DUMP_H
#define ARKI_PYTHON_ARKI_DUMP_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
namespace runtime {
struct ArkiDump;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::runtime::ArkiDump* arki_dump;
} arkipy_ArkiDump;

extern PyTypeObject* arkipy_ArkiDump_Type;

#define arkipy_ArkiDump_Check(ob) \
    (Py_TYPE(ob) == &arkipy_ArkiDump_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_ArkiDump_Type))
}

namespace arki {
namespace python {

void register_arki_dump(PyObject* m);

}
}

#endif
