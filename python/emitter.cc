#include "emitter.h"
#include "arki/core/time.h"

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


emitter::NodeType PythonReader::type() const
{
    throw std::runtime_error("not implemented");
}

bool PythonReader::as_bool(const char* desc) const
{
    throw std::runtime_error("not implemented");
}

long long int PythonReader::as_int(const char* desc) const
{
    throw std::runtime_error("not implemented");
}

double PythonReader::as_double(const char* desc) const
{
    throw std::runtime_error("not implemented");
}

std::string PythonReader::as_string(const char* desc) const
{
    throw std::runtime_error("not implemented");
}


unsigned PythonReader::list_size(const char* desc) const
{
    throw std::runtime_error("not implemented");
}

bool PythonReader::as_bool(unsigned idx, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

long long int PythonReader::as_int(unsigned idx, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

double PythonReader::as_double(unsigned idx, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

std::string PythonReader::as_string(unsigned idx, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

void PythonReader::sub(unsigned idx, const char* desc, std::function<void(const Reader&)>) const
{
    throw std::runtime_error("not implemented");
}


bool PythonReader::has_key(const std::string& key, emitter::NodeType type) const
{
    throw std::runtime_error("not implemented");
}

bool PythonReader::as_bool(const std::string& key, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

long long int PythonReader::as_int(const std::string& key, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

double PythonReader::as_double(const std::string& key, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

std::string PythonReader::as_string(const std::string& key, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

core::Time PythonReader::as_time(const std::string& key, const char* desc) const
{
    throw std::runtime_error("not implemented");
}

void PythonReader::items(const char* desc, std::function<void(const std::string&, const Reader&)>) const
{
    throw std::runtime_error("not implemented");
}

void PythonReader::sub(const std::string& key, const char* desc, std::function<void(const Reader&)>) const
{
    throw std::runtime_error("not implemented");
}

}
}
