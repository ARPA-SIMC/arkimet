#ifndef ARKI_PYTHON_SUMMARY_H
#define ARKI_PYTHON_SUMMARY_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {
struct Summary;
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::Summary* summary;
} arkipy_Summary;

extern PyTypeObject* arkipy_Summary_Type;

#define arkipy_Summary_Check(ob) \
    (Py_TYPE(ob) == arkipy_Summary_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_Summary_Type))
}

namespace arki {
namespace python {

arkipy_Summary* summary_create();
arkipy_Summary* summary_create(std::unique_ptr<Summary>&& summary);

void register_summary(PyObject* m);

}
}
#endif


