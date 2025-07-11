#ifndef ARKI_PYTHON_MATCHER_H
#define ARKI_PYTHON_MATCHER_H

#define PY_SSIZE_T_CLEAN
#include "arki/dataset/fwd.h"
#include "arki/matcher.h"
#include "utils/values.h"
#include <Python.h>
#include <memory>

extern "C" {

typedef struct
{
    PyObject_HEAD arki::Matcher matcher;
} arkipy_Matcher;

extern PyTypeObject* arkipy_Matcher_Type;

#define arkipy_Matcher_Check(ob)                                               \
    (Py_TYPE(ob) == arkipy_Matcher_Type ||                                     \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_Matcher_Type))
}

namespace arki {
namespace python {

PyObject* matcher_to_python(arki::Matcher matcher);
inline PyObject* to_python(arki::Matcher matcher)
{
    return matcher_to_python(matcher);
}

/**
 * Return a Matcher from a python string or arkimet.Matcher object
 */
arki::Matcher
matcher_from_python(std::shared_ptr<arki::dataset::Session> session,
                    PyObject* o);

void register_matcher(PyObject* m);

} // namespace python
} // namespace arki

#endif
