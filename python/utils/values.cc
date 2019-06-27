#include "values.h"
#include "core.h"
#include <string>

namespace arki {
namespace python {

PyObject* string_to_python(const char* str)
{
    return throw_ifnull(PyUnicode_FromString(str));
}

PyObject* string_to_python(const std::string& str)
{
    return throw_ifnull(PyUnicode_FromStringAndSize(str.data(), str.size()));
}

std::string string_from_python(PyObject* o)
{
    if (!PyUnicode_Check(o))
    {
        PyErr_SetString(PyExc_TypeError, "value must be an instance of str");
        throw PythonException();
    }
    ssize_t size;
    const char* res = throw_ifnull(PyUnicode_AsUTF8AndSize(o, &size));
    return std::string(res, size);
}

const char* cstring_from_python(PyObject* o)
{
    if (PyUnicode_Check(o))
        return throw_ifnull(PyUnicode_AsUTF8(o));
    PyErr_SetString(PyExc_TypeError, "value must be an instance of str");
    throw PythonException();
}

int int_from_python(PyObject* o)
{
    int res = PyLong_AsLong(o);
    if (PyErr_Occurred())
        throw PythonException();
    return res;
}

PyObject* int_to_python(int val)
{
    return throw_ifnull(PyLong_FromLong(val));
}

double double_from_python(PyObject* o)
{
    double res = PyFloat_AsDouble(o);
    if (res == -1.0 && PyErr_Occurred())
        throw PythonException();
    return res;
}

PyObject* double_to_python(double val)
{
    return throw_ifnull(PyFloat_FromDouble(val));
}

}
}
