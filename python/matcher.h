#ifndef ARKI_PYTHON_MATCHER_H
#define ARKI_PYTHON_MATCHER_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>
#include "arki/matcher.h"
#include "utils/values.h"

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

PyObject* matcher_to_python(arki::Matcher matcher);
inline PyObject* to_python(arki::Matcher matcher) { return matcher_to_python(matcher); }


/**
 * Return a Matcher from a python string or arkimet.Matcher object
 */
arki::Matcher matcher_from_python(PyObject* o);
template<> inline arki::Matcher from_python<arki::Matcher>(PyObject* o) { return matcher_from_python(o); }

void register_matcher(PyObject* m);

}
}

#endif
