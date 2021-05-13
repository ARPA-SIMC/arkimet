#include "structured.h"
#include "utils/values.h"
#include "arki/core/time.h"
#include <datetime.h>

namespace arki {
namespace python {

PythonEmitter::~PythonEmitter()
{
    while (!stack.empty())
    {
        Py_XDECREF(stack.back().o);
        stack.pop_back();
    }
}

void PythonEmitter::add_object(pyo_unique_ptr o)
{
    if (stack.empty())
    {
        if (res)
            throw std::runtime_error("root element emitted twice, outside a container");
        res = std::move(o);
    } else switch (stack.back().state) {
        case Target::LIST:
            if (PyList_Append(stack.back().o, o) == -1)
                throw PythonException();
            break;
        case Target::MAPPING:
            stack.push_back(Target(Target::MAPPING_KEY, o.release()));
            break;
        case Target::MAPPING_KEY:
        {
            pyo_unique_ptr key(stack.back().o);
            stack.pop_back();
            if (PyDict_SetItem(stack.back().o, key, o) == -1)
                throw PythonException();
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
    pyo_unique_ptr o(stack.back().o);
    stack.pop_back();
    add_object(std::move(o));
}

void PythonEmitter::start_mapping()
{
    pyo_unique_ptr o(throw_ifnull(PyDict_New()));
    stack.push_back(Target(Target::MAPPING, o.release()));
}

void PythonEmitter::end_mapping()
{
    pyo_unique_ptr o(stack.back().o);
    stack.pop_back();
    add_object(std::move(o));
}

void PythonEmitter::add_null()
{
    pyo_unique_ptr none(Py_None);
    none.incref();
    add_object(std::move(none));
}

void PythonEmitter::add_bool(bool val)
{
    pyo_unique_ptr res;
    if (val)
        res.reset(Py_True);
    else
        res.reset(Py_False);
    res.incref();
    add_object(std::move(res));
}

void PythonEmitter::add_int(long long int val)
{
    pyo_unique_ptr o(throw_ifnull(PyLong_FromLong(val)));
    add_object(std::move(o));
}

void PythonEmitter::add_double(double val)
{
    pyo_unique_ptr o(throw_ifnull(PyFloat_FromDouble(val)));
    add_object(std::move(o));
}

void PythonEmitter::add_string(const std::string& val)
{
    pyo_unique_ptr o(throw_ifnull(PyUnicode_FromStringAndSize(val.data(), val.size())));
    add_object(std::move(o));
}

void PythonEmitter::add_time(const core::Time& val)
{
    pyo_unique_ptr o(throw_ifnull(PyDateTime_FromDateAndTime(
            val.ye, val.mo, val.da,
            val.ho, val.mi, val.se, 0)));
    add_object(std::move(o));
}


/*
 * PythonReader
 */

arki::structured::NodeType PythonReader::type() const
{
    if (o == Py_None)
        return arki::structured::NodeType::NONE;
    if (PyBool_Check(o))
        return arki::structured::NodeType::BOOL;
    if (PyLong_Check(o))
        return arki::structured::NodeType::INT;
    if (PyFloat_Check(o))
        return arki::structured::NodeType::DOUBLE;
    if (PyUnicode_Check(o))
        return arki::structured::NodeType::STRING;
    if (PyMapping_Check(o))
        return arki::structured::NodeType::MAPPING;
    if (PySequence_Check(o))
        return arki::structured::NodeType::LIST;
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


bool PythonReader::has_key(const std::string& key, arki::structured::NodeType type) const
{
    pyo_unique_ptr el(PyMapping_GetItemString(o, key.c_str()));
    if (!el)
    {
        PyErr_Clear();
        return 0;
    }
    switch (type)
    {
        case arki::structured::NodeType::NONE:
            return el == Py_None;
        case arki::structured::NodeType::BOOL:
            return el == Py_True || el == Py_False;
        case arki::structured::NodeType::INT:
            return PyLong_Check(el);
        case arki::structured::NodeType::STRING:
            return PyUnicode_Check(el);
        case arki::structured::NodeType::MAPPING:
            return PyMapping_Check(el);
        case arki::structured::NodeType::LIST:
            return PySequence_Check(el);
        default:
            return 0;
    }
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

static int get_attr_int(PyObject* o, const char* name)
{
    pyo_unique_ptr res(throw_ifnull(PyObject_GetAttrString(o, name)));
    return from_python<int>(res);
}

core::Time PythonReader::as_time(const std::string& key, const char* desc) const
{
    pyo_unique_ptr el(throw_ifnull(PyMapping_GetItemString(o, key.c_str())));
    if (PyDateTime_Check(el))
    {
        int ye = PyDateTime_GET_YEAR(el.get());
        int mo = PyDateTime_GET_MONTH(el.get());
        int da = PyDateTime_GET_DAY(el.get());
        int ho = PyDateTime_DATE_GET_HOUR(el.get());
        int mi = PyDateTime_DATE_GET_MINUTE(el.get());
        int se = PyDateTime_DATE_GET_SECOND(el.get());
        return core::Time(ye, mo, da, ho, mi, se);
    }
    else if (PySequence_Check(el))
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
    else
    {
        // Fall back to duck typing, to catch creative cases such as
        // cftime.datetime instances. See https://unidata.github.io/cftime/api.html
        return core::Time(
            get_attr_int(el, "year"),
            get_attr_int(el, "month"),
            get_attr_int(el, "day"),
            get_attr_int(el, "hour"),
            get_attr_int(el, "minute"),
            get_attr_int(el, "second")
        );
    }

    // PyErr_SetString(PyExc_NotImplementedError, "Cannot parse a non-sequence, non-datetime as a time");
    // throw PythonException();
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

namespace structured{

void init()
{
    /*
     * PyDateTimeAPI, that is used by all the PyDate* and PyTime* macros, is
     * defined as a static variable defaulting to NULL, and it needs to be
     * initialized on each and every C file where it is used.
     *
     * Therefore, we need to have a common_init() to call from all
     * initialization functions. *sigh*
     */
    if (!PyDateTimeAPI)
        PyDateTime_IMPORT;
}

}

}
}
