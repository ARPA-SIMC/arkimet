#ifndef ARKI_PYTHON_MATCHER_H
#define ARKI_PYTHON_MATCHER_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>
#include "arki/matcher.h"

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::Matcher matcher;
} arkipy_Matcher;

extern PyTypeObject* arkipy_Matcher_Type;

#define arkipy_Matcher_Check(ob) \
    (Py_TYPE(ob) == arkipy_Matcher_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_Matcher_Type))

}

namespace arki {
namespace python {

PyObject* matcher(arki::Matcher matcher);

void register_matcher(PyObject* m);

}
}

#endif
