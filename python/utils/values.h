#ifndef ARKI_PYTHON_VALUES_H
#define ARKI_PYTHON_VALUES_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace arki {
namespace python {

/// Base template for all from_python shortcuts
template<typename T> inline T from_python(PyObject*) { throw std::runtime_error("method not implemented"); }

/// Convert an utf8 string to a python str object
PyObject* cstring_to_python(const char* str);
inline PyObject* to_python(const char* s) { return cstring_to_python(s); }

/// Convert an utf8 string to a python str object
PyObject* string_to_python(const std::string& str);
inline PyObject* to_python(const std::string& s) { return string_to_python(s); }

/// Convert a python string, bytes or unicode to an utf8 string
std::string string_from_python(PyObject* o);
template<> inline std::string from_python<std::string>(PyObject* o) { return string_from_python(o); }

/// Convert a python string, bytes or unicode to an utf8 string
const char* cstring_from_python(PyObject* o);
template<> inline const char* from_python<const char*>(PyObject* o) { return cstring_from_python(o); }

/// Convert a Python object to an int
int int_from_python(PyObject* o);
template<> inline int from_python<int>(PyObject* o) { return int_from_python(o); }

/// Convert an int to a Python object
PyObject* int_to_python(int val);
inline PyObject* to_python(int val) { return int_to_python(val); }

/// Convert a Python object to a double
double double_from_python(PyObject* o);
template<> inline double from_python<double>(PyObject* o) { return double_from_python(o); }

/// Convert a double to a Python object
PyObject* double_to_python(double val);
inline PyObject* to_python(double val) { return double_to_python(val); }

/// Read a string list from a Python object
std::vector<std::string> stringlist_from_python(PyObject* o);
template<> inline std::vector<std::string> from_python<std::vector<std::string>>(PyObject* o)
{
    return stringlist_from_python(o);
}


}
}

#endif
