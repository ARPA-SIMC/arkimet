#ifndef ARKI_PYTHON_ARKI_SCAN_H
#define ARKI_PYTHON_ARKI_SCAN_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
namespace runtime {
struct ArkiScan;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::runtime::ArkiScan* arki_scan;
} arkipy_ArkiScan;

extern PyTypeObject* arkipy_ArkiScan_Type;

#define arkipy_ArkiScan_Check(ob) \
    (Py_TYPE(ob) == &arkipy_ArkiScan_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_ArkiScan_Type))
}

namespace arki {
namespace python {

void register_arki_scan(PyObject* m);

}
}

#endif
