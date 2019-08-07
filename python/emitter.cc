#include "emitter.h"
#include "utils/values.h"
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


structured::NodeType PythonReader::type() const
{
    if (o == Py_None)
        return structured::NodeType::NONE;
    if (PyBool_Check(o))
        return structured::NodeType::BOOL;
    if (PyLong_Check(o))
        return structured::NodeType::INT;
    if (PyUnicode_Check(o))
        return structured::NodeType::STRING;
    if (PyMapping_Check(o))
        return structured::NodeType::MAPPING;
    if (PySequence_Check(o))
        return structured::NodeType::LIST;
    throw std::invalid_argument("python object " + repr() + " cannot be understood");
}

std::string PythonReader::repr() const
{
    py_unique_ptr<PyObject> py_repr(PyObject_Repr(o));
    if (!py_repr)
    {
        PyErr_Clear();
        return "(python repr failed)";
    }
    Py_ssize_t size;
    const char* res = PyUnicode_AsUTF8AndSize(py_repr, &size);
    return std::string(res, size);
}

bool PythonReader::as_bool(const char* desc) const
{
    return from_python<bool>(o);
}

long long int PythonReader::as_int(const char* desc) const
{
    long long int res = PyLong_AsLongLong(o);
    if (res == -1 && PyErr_Occurred())
        throw PythonException();
    return res;
}

double PythonReader::as_double(const char* desc) const
{
    return from_python<double>(o);
}

std::string PythonReader::as_string(const char* desc) const
{
    return from_python<std::string>(o);
}


unsigned PythonReader::list_size(const char* desc) const
{
    int res = PySequence_Size(o);
    if (res == -1) throw PythonException();
    return res;
}

bool PythonReader::as_bool(unsigned idx, const char* desc) const
{
    pyo_unique_ptr res(throw_ifnull(PySequence_GetItem(o, idx)));
    return from_python<bool>(res);
}

long long int PythonReader::as_int(unsigned idx, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PySequence_GetItem(o, idx)));
    long long int res = PyLong_AsLongLong(el);
    if (res == -1 && PyErr_Occurred())
        throw PythonException();
    return res;
}

double PythonReader::as_double(unsigned idx, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PySequence_GetItem(o, idx)));
    return from_python<double>(el);
}

std::string PythonReader::as_string(unsigned idx, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PySequence_GetItem(o, idx)));
    return from_python<std::string>(el);
}

void PythonReader::sub(unsigned idx, const char* desc, std::function<void(const Reader&)> dest) const
{
    pyo_unique_ptr el(throw_ifnull(PySequence_GetItem(o, idx)));
    PythonReader reader(el);
    dest(reader);
}


bool PythonReader::has_key(const std::string& key, structured::NodeType type) const
{
    int res = PyMapping_HasKeyString(o, key.c_str());
    return res == 1;
}

bool PythonReader::as_bool(const std::string& key, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PyMapping_GetItemString(o, key.c_str())));
    return from_python<bool>(el);
}

long long int PythonReader::as_int(const std::string& key, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PyMapping_GetItemString(o, key.c_str())));
    long long int res = PyLong_AsLongLong(el);
    if (res == -1 && PyErr_Occurred())
        throw PythonException();
    return res;
}

double PythonReader::as_double(const std::string& key, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PyMapping_GetItemString(o, key.c_str())));
    return from_python<double>(el);
}

std::string PythonReader::as_string(const std::string& key, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PyMapping_GetItemString(o, key.c_str())));
    return from_python<std::string>(el);
}

core::Time PythonReader::as_time(const std::string& key, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PyMapping_GetItemString(o, key.c_str())));
    if (PySequence_Check(el))
    {
        Py_ssize_t size = PySequence_Size(el);
        if (size == -1)
            throw PythonException();
        if (size != 6)
        {
            PyErr_Format(PyExc_ValueError, "time should be a sequence of 6 elements, not %d", (int)size);
            throw PythonException();
        }
        pyo_unique_ptr py_year(throw_ifnull(PySequence_GetItem(el, 0)));
        int year = from_python<int>(py_year);
        pyo_unique_ptr py_month(throw_ifnull(PySequence_GetItem(el, 1)));
        int month = from_python<int>(py_month);
        pyo_unique_ptr py_day(throw_ifnull(PySequence_GetItem(el, 2)));
        int day = from_python<int>(py_day);
        pyo_unique_ptr py_hour(throw_ifnull(PySequence_GetItem(el, 3)));
        int hour = from_python<int>(py_hour);
        pyo_unique_ptr py_minute(throw_ifnull(PySequence_GetItem(el, 4)));
        int minute = from_python<int>(py_minute);
        pyo_unique_ptr py_second(throw_ifnull(PySequence_GetItem(el, 5)));
        int second = from_python<int>(py_second);
        return core::Time(year, month, day, hour, minute, second);
    }
    PyErr_SetString(PyExc_NotImplementedError, "Cannot parse a non-sequence as a time");
    throw PythonException();
}

void PythonReader::items(const char* desc, std::function<void(const std::string&, const Reader&)> dest) const
{
    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while (PyDict_Next(o, &pos, &key, &value)) {
        PythonReader reader(value);
        dest(from_python<std::string>(key), reader);
    }
}

void PythonReader::sub(const std::string& key, const char* desc, std::function<void(const Reader&)> dest) const
{
    pyo_unique_ptr el(throw_ifnull(PyMapping_GetItemString(o, key.c_str())));
    PythonReader reader(el);
    dest(reader);
}

}
}
