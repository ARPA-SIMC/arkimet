#ifndef ARKI_PYTHON_COUNTERS_H
#define ARKI_PYTHON_COUNTERS_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
namespace utils {
namespace acct {
class Counter;
}
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::utils::acct::Counter* counter;
} arkipy_countersCounter;

extern PyTypeObject* arkipy_countersCounter_Type;

#define arkipy_countersCounter_Check(ob) \
    (Py_TYPE(ob) == arkipy_countersCounter_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_countersCounter_Type))

}

namespace arki {
namespace python {

void register_counters(PyObject* m);

}
}

#endif
