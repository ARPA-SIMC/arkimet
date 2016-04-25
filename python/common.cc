#include "common.h"
#include <Python.h>
//#include <datetime.h>
#include "arki/libconfig.h"

#if PY_MAJOR_VERSION >= 3
    #define PyInt_FromLong PyLong_FromLong
    #define PyInt_AsLong PyLong_AsLong
    #define PyInt_Type PyLong_Type
#endif

namespace arki {
namespace python {

#if 0
wrpy_c_api* wrpy = 0;

PyObject* format_varcode(wreport::Varcode code)
{
    char buf[7];
    snprintf(buf, 7, "%c%02d%03d",
            WR_VAR_F(code) == 0 ? 'B' :
            WR_VAR_F(code) == 1 ? 'R' :
            WR_VAR_F(code) == 2 ? 'C' :
            WR_VAR_F(code) == 3 ? 'D' : '?',
            WR_VAR_X(code), WR_VAR_Y(code));
    return PyUnicode_FromString(buf);
}

void set_wreport_exception(const wreport::error& e)
{
    switch (e.code())
    {
        case WR_ERR_NONE:
            PyErr_SetString(PyExc_SystemError, e.what());
            break;
        case WR_ERR_NOTFOUND:    // Item not found
            PyErr_SetString(PyExc_KeyError, e.what());
            break;
        case WR_ERR_TYPE:        // Wrong variable type
            PyErr_SetString(PyExc_TypeError, e.what());
            break;
        case WR_ERR_ALLOC:       // Cannot allocate memory
            PyErr_SetString(PyExc_MemoryError, e.what());
            break;
        case WR_ERR_ODBC:        // ODBC error
            PyErr_SetString(PyExc_OSError, e.what());
            break;
        case WR_ERR_HANDLES:     // Handle management error
            PyErr_SetString(PyExc_SystemError, e.what());
            break;
        case WR_ERR_TOOLONG:     // Buffer is too short to fit data
            PyErr_SetString(PyExc_ValueError, e.what());
            break;
        case WR_ERR_SYSTEM:      // Error reported by the system
            PyErr_SetString(PyExc_OSError, e.what());
            break;
        case WR_ERR_CONSISTENCY: // Consistency check failed
            PyErr_SetString(PyExc_RuntimeError, e.what());
            break;
        case WR_ERR_PARSE:       // Parse error
            PyErr_SetString(PyExc_ValueError, e.what());
            break;
        case WR_ERR_WRITE:       // Write error
            PyErr_SetString(PyExc_RuntimeError, e.what());
            break;
        case WR_ERR_REGEX:       // Regular expression error
            PyErr_SetString(PyExc_ValueError, e.what());
            break;
        case WR_ERR_UNIMPLEMENTED: // Feature not implemented
            PyErr_SetString(PyExc_NotImplementedError, e.what());
            break;
        case WR_ERR_DOMAIN:      // Value outside acceptable domain
            PyErr_SetString(PyExc_OverflowError, e.what());
            break;
        default:
            PyErr_Format(PyExc_SystemError, "unhandled exception with code %d: %s", e.code(), e.what());
            break;
    }
}

PyObject* raise_wreport_exception(const wreport::error& e)
{
    set_wreport_exception(e);
    return nullptr;
}

void set_std_exception(const std::exception& e)
{
    PyErr_SetString(PyExc_RuntimeError, e.what());
}

PyObject* raise_std_exception(const std::exception& e)
{
    set_std_exception(e);
    return NULL;
}

PyObject* datetime_to_python(const Datetime& dt)
{
    if (dt.is_missing())
    {
        Py_INCREF(Py_None);
        return Py_None;
    }

    return PyDateTime_FromDateAndTime(
            dt.year, dt.month,  dt.day,
            dt.hour, dt.minute, dt.second, 0);
}

int datetime_from_python(PyObject* dt, Datetime& out)
{
    if (dt == NULL || dt == Py_None)
    {
        out = Datetime();
        return 0;
    }

    if (!PyDateTime_Check(dt))
    {
        PyErr_SetString(PyExc_TypeError, "value must be an instance of datetime.datetime");
        return -1;
    }

    out = Datetime(
        PyDateTime_GET_YEAR((PyDateTime_DateTime*)dt),
        PyDateTime_GET_MONTH((PyDateTime_DateTime*)dt),
        PyDateTime_GET_DAY((PyDateTime_DateTime*)dt),
        PyDateTime_DATE_GET_HOUR((PyDateTime_DateTime*)dt),
        PyDateTime_DATE_GET_MINUTE((PyDateTime_DateTime*)dt),
        PyDateTime_DATE_GET_SECOND((PyDateTime_DateTime*)dt));
    return 0;
}

int datetimerange_from_python(PyObject* val, DatetimeRange& out)
{
    if (PySequence_Size(val) != 2)
    {
        PyErr_SetString(PyExc_TypeError, "Expected a 2-tuple of datetime() objects");
        return -1;
    }
    pyo_unique_ptr dtmin(PySequence_GetItem(val, 0));
    pyo_unique_ptr dtmax(PySequence_GetItem(val, 1));

    if (datetime_from_python(dtmin, out.min))
        return -1;
    if (datetime_from_python(dtmax, out.max))
        return -1;

    return 0;
}

namespace {

/// Convert an integer to Python, returning None if it is MISSING_INT
PyObject* dballe_int_to_python(int val)
{
    if (val == MISSING_INT)
    {
        Py_INCREF(Py_None);
        return Py_None;
    } else
        return PyInt_FromLong(val);
}

/// Convert a Python object to an integer, returning MISSING_INT if it is None
int dballe_int_from_python(PyObject* o, int& out)
{
    if (o == NULL || o == Py_None)
    {
        out = MISSING_INT;
        return 0;
    }

    int res = PyInt_AsLong(o);
    if (res == -1 && PyErr_Occurred())
        return -1;

    out = res;
    return 0;
}

}

PyObject* level_to_python(const Level& lev)
{
    if (lev.is_missing())
    {
        Py_INCREF(Py_None);
        return Py_None;
    }

    PyObject* res = PyTuple_New(4);
    if (!res) return NULL;

    if (PyObject* v = dballe_int_to_python(lev.ltype1))
        PyTuple_SET_ITEM(res, 0, v);
    else {
        Py_DECREF(res);
        return NULL;
    }

    if (PyObject* v = dballe_int_to_python(lev.l1))
        PyTuple_SET_ITEM(res, 1, v);
    else {
        Py_DECREF(res);
        return NULL;
    }

    if (PyObject* v = dballe_int_to_python(lev.ltype2))
        PyTuple_SET_ITEM(res, 2, v);
    else {
        Py_DECREF(res);
        return NULL;
    }

    if (PyObject* v = dballe_int_to_python(lev.l2))
        PyTuple_SET_ITEM(res, 3, v);
    else {
        Py_DECREF(res);
        return NULL;
    }

    return res;
}

int level_from_python(PyObject* o, Level& out)
{
    if (o == NULL || o == Py_None)
    {
        out = Level();
        return 0;
    }

    if (!PyTuple_Check(o))
    {
        PyErr_SetString(PyExc_TypeError, "level must be a tuple");
        return -1;
    }

    unsigned size = PyTuple_Size(o);
    if (size > 4)
    {
        PyErr_SetString(PyExc_TypeError, "level tuple must have at most 4 elements");
        return -1;
    }

    Level res;
    if (size < 1) { out = res; return 0; }

    if (int err = dballe_int_from_python(PyTuple_GET_ITEM(o, 0), res.ltype1)) return err;
    if (size < 2) { out = res; return 0; }

    if (int err = dballe_int_from_python(PyTuple_GET_ITEM(o, 1), res.l1)) return err;
    if (size < 3) { out = res; return 0; }

    if (int err = dballe_int_from_python(PyTuple_GET_ITEM(o, 2), res.ltype2)) return err;
    if (size < 4) { out = res; return 0; }

    if (int err = dballe_int_from_python(PyTuple_GET_ITEM(o, 3), res.l2)) return err;
    out = res;
    return 0;
}

PyObject* trange_to_python(const Trange& tr)
{
    if (tr.is_missing())
    {
        Py_INCREF(Py_None);
        return Py_None;
    }

    PyObject* res = PyTuple_New(3);
    if (!res) return NULL;

    if (PyObject* v = dballe_int_to_python(tr.pind))
        PyTuple_SET_ITEM(res, 0, v);
    else {
        Py_DECREF(res);
        return NULL;
    }

    if (PyObject* v = dballe_int_to_python(tr.p1))
        PyTuple_SET_ITEM(res, 1, v);
    else {
        Py_DECREF(res);
        return NULL;
    }

    if (PyObject* v = dballe_int_to_python(tr.p2))
        PyTuple_SET_ITEM(res, 2, v);
    else {
        Py_DECREF(res);
        return NULL;
    }

    return res;
}

int trange_from_python(PyObject* o, Trange& out)
{
    if (o == NULL || o == Py_None)
    {
        out = Trange();
        return 0;
    }

    if (!PyTuple_Check(o))
    {
        PyErr_SetString(PyExc_TypeError, "time range must be a tuple");
        return -1;
    }

    unsigned size = PyTuple_Size(o);
    if (size > 3)
    {
        PyErr_SetString(PyExc_TypeError, "time range tuple must have at most 3 elements");
        return -1;
    }

    Trange res;
    if (size < 1) { out = res; return 0; }

    if (int err = dballe_int_from_python(PyTuple_GET_ITEM(o, 0), res.pind)) return err;
    if (size < 2) { out = res; return 0; }

    if (int err = dballe_int_from_python(PyTuple_GET_ITEM(o, 1), res.p1)) return err;
    if (size < 3) { out = res; return 0; }

    if (int err = dballe_int_from_python(PyTuple_GET_ITEM(o, 2), res.p2)) return err;
    out = res;
    return 0;
}

int string_from_python(PyObject* o, std::string& out)
{
#if PY_MAJOR_VERSION >= 3
    if (PyBytes_Check(o)) {
        const char* v = PyBytes_AsString(o);
        if (v == NULL) return -1;
        out = v;
        return 0;
    }
#else
    if (PyString_Check(o)) {
        const char* v = PyString_AsString(o);
        if (v == NULL) return -1;
        out = v;
        return 0;
    }
#endif
    if (PyUnicode_Check(o)) {
#if PY_MAJOR_VERSION >= 3
        const char* v = PyUnicode_AsUTF8(o);
        if (v == NULL) return -1;
        out = v;
        return 0;
#else
        PyObject *utf8 = PyUnicode_AsUTF8String(o);
        const char* v = PyString_AsString(utf8);
        if (v == NULL)
        {
            Py_DECREF(utf8);
            return -1;
        }
        out = v;
        Py_DECREF(utf8);
        return 0;
#endif
    }
    PyErr_SetString(PyExc_TypeError, "value must be an instance of str, bytes or unicode");
    return -1;
}

int file_get_fileno(PyObject* o)
{
    // fileno_value = obj.fileno()
    pyo_unique_ptr fileno_meth(PyObject_GetAttrString(o, "fileno"));
    if (!fileno_meth) return -1;
    pyo_unique_ptr fileno_args(Py_BuildValue("()"));
    if (!fileno_args) return -1;
    PyObject* fileno_value = PyObject_Call(fileno_meth, fileno_args, NULL);
    if (!fileno_value)
    {
        if (PyErr_ExceptionMatches(PyExc_AttributeError) || PyErr_ExceptionMatches(PyExc_IOError))
            PyErr_Clear();
        return -1;
    }

    // fileno = int(fileno_value)
    if (!PyObject_TypeCheck(fileno_value, &PyInt_Type)) {
        PyErr_SetString(PyExc_ValueError, "fileno() function must return an integer");
        return -1;
    }

    return PyInt_AsLong(fileno_value);
}

PyObject* file_get_data(PyObject* o, char*&buf, Py_ssize_t& len)
{
    // Use read() instead
    pyo_unique_ptr read_meth(PyObject_GetAttrString(o, "read"));
    pyo_unique_ptr read_args(Py_BuildValue("()"));
    pyo_unique_ptr data(PyObject_Call(read_meth, read_args, NULL));
    if (!data) return nullptr;

#if PY_MAJOR_VERSION >= 3
    if (!PyObject_TypeCheck(data, &PyBytes_Type)) {
        PyErr_SetString(PyExc_ValueError, "read() function must return a bytes object");
        return nullptr;
    }
    if (PyBytes_AsStringAndSize(data, &buf, &len))
        return nullptr;
#else
    if (!PyObject_TypeCheck(data, &PyString_Type)) {
        Py_DECREF(data);
        PyErr_SetString(PyExc_ValueError, "read() function must return a string object");
        return nullptr;
    }
    if (PyString_AsStringAndSize(data, &buf, &len))
        return nullptr;
#endif

    return data.release();
}


int object_repr(PyObject* o, std::string& out)
{
    pyo_unique_ptr fileno_repr(PyObject_Repr(o));
    if (!fileno_repr) return -1;

    std::string name;
    if (string_from_python(fileno_repr, name))
        return -1;

    return 0;
}

int common_init()
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

    if (!wrpy)
    {
        wrpy = (wrpy_c_api*)PyCapsule_Import("_wreport._C_API", 0);
        if (!wrpy)
            return -1;
    }

    return 0;
}
#endif

}
}
