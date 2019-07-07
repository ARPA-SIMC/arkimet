#ifndef ARKI_PYTHON_DICT_H
#define ARKI_PYTHON_DICT_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/python/core.h>
#include <arki/python/values.h>

namespace arki {
namespace python {

template<typename T>
inline void set_dict(PyObject* dict, const char* key, const T& val)
{
    auto pyval = to_python(val);
    if (PyDict_SetItemString(dict, key, pyval))
        throw PythonException();
}

inline void set_dict(PyObject* dict, const char* key, PyObject* val)
{
    if (PyDict_SetItemString(dict, key, val))
        throw PythonException();
}


}
}

#endif
