#include "emitter.h"

namespace arki {
namespace python {

PythonEmitter::~PythonEmitter()
{
    if (res) Py_DECREF(res);
    while (!stack.empty())
    {
        if (stack.back().o) Py_DECREF(stack.back().o);
        stack.pop_back();
    }
}

void PythonEmitter::add_object(PyObject* o)
{
    if (stack.empty())
    {
        if (res)
        {
            Py_DECREF(o);
            throw std::runtime_error("root element emitted twice, outside a container");
        }
        res = o;
    } else switch (stack.back().state) {
        case Target::LIST:
            if (PyList_Append(stack.back().o, o) == -1)
                throw PythonException();
            Py_DECREF(o);
            break;
        case Target::MAPPING:
            stack.push_back(Target(Target::MAPPING_KEY, o));
            break;
        case Target::MAPPING_KEY:
        {
            PyObject* key = stack.back().o;
            stack.pop_back();
            if (PyDict_SetItem(stack.back().o, key, o) == -1)
                throw PythonException();
            Py_DECREF(key);
            Py_DECREF(o);
            break;
        }
    }
}

void PythonEmitter::start_list()
{
    PyObject* o = PyList_New(0);
    if (o == nullptr) throw PythonException();
    stack.push_back(Target(Target::LIST, o));
}

void PythonEmitter::end_list()
{
    PyObject* o = stack.back().o;
    stack.pop_back();
    add_object(o);
}

void PythonEmitter::start_mapping()
{
    PyObject* o = PyDict_New();
    if (o == nullptr) throw PythonException();
    stack.push_back(Target(Target::MAPPING, o));
}

void PythonEmitter::end_mapping()
{
    PyObject* o = stack.back().o;
    stack.pop_back();
    add_object(o);
}

void PythonEmitter::add_null()
{
    Py_INCREF(Py_None);
    add_object(Py_None);
}

void PythonEmitter::add_bool(bool val)
{
    if (val)
    {
        Py_INCREF(Py_True);
        add_object(Py_True);
    } else {
        Py_INCREF(Py_False);
        add_object(Py_False);
    }
}

void PythonEmitter::add_int(long long int val)
{
    PyObject* o = PyLong_FromLong(val);
    if (o == nullptr) throw PythonException();
    add_object(o);
}

void PythonEmitter::add_double(double val)
{
    PyObject* o = PyFloat_FromDouble(val);
    if (o == nullptr) throw PythonException();
    add_object(o);
}

void PythonEmitter::add_string(const std::string& val)
{
    PyObject* o = PyUnicode_FromStringAndSize(val.data(), val.size());
    if (o == nullptr) throw PythonException();
    add_object(o);
}


}
}
