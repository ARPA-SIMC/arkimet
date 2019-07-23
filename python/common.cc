#include "common.h"
#include "cfg.h"
#include "metadata.h"
#include "matcher.h"
#include "utils/values.h"
#include "arki/libconfig.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/init.h"

using namespace std;

namespace arki {
namespace python {

void set_std_exception(const std::exception& e)
{
    PyErr_SetString(PyExc_RuntimeError, e.what());
}

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


#if 0
wrpy_c_api* wrpy = 0;


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
#endif


int object_repr(PyObject* o, std::string& out)
{
    pyo_unique_ptr fileno_repr(PyObject_Repr(o));
    if (!fileno_repr) return -1;

    out = string_from_python(fileno_repr);
    return 0;
}

int file_get_fileno(PyObject* o)
{
    // If it is already an int handle, return it
    if (PyLong_Check(o))
        return PyLong_AsLong(o);

    // fileno_value = obj.fileno()
    pyo_unique_ptr fileno_meth(PyObject_GetAttrString(o, "fileno"));
    if (!fileno_meth) return -1;
    pyo_unique_ptr fileno_args(Py_BuildValue("()"));
    if (!fileno_args) return -1;
    pyo_unique_ptr fileno_value(PyObject_CallObject(fileno_meth, fileno_args));
    if (!fileno_value) return -1;

    // fileno = int(fileno_value)
    if (!PyLong_Check(fileno_value))
    {
        PyErr_SetString(PyExc_ValueError, "fileno() function must return an integer");
        return -1;
    }

    return PyLong_AsLong(fileno_value);
}

core::cfg::Section section_from_python(PyObject* o)
{
    try {
        if (arkipy_cfgSection_Check(o)) {
            return *((arkipy_cfgSection*)o)->section;
        }

        if (PyBytes_Check(o)) {
            const char* v = throw_ifnull(PyBytes_AsString(o));
            return core::cfg::Section::parse(v);
        }
        if (PyUnicode_Check(o)) {
            const char* v = throw_ifnull(PyUnicode_AsUTF8(o));
            return core::cfg::Section::parse(v);
        }
        if (PyDict_Check(o))
        {
            core::cfg::Section res;
            PyObject *key, *val;
            Py_ssize_t pos = 0;
            while (PyDict_Next(o, &pos, &key, &val))
                res.set(string_from_python(key), string_from_python(val));
            return res;
        }
        PyErr_SetString(PyExc_TypeError, "value must be an instance of str, bytes, or dict");
        throw PythonException();
    } ARKI_CATCH_RETHROW_PYTHON
}

core::cfg::Sections sections_from_python(PyObject* o)
{
    try {
        if (arkipy_cfgSections_Check(o)) {
            return ((arkipy_cfgSections*)o)->sections;
        }

        if (PyBytes_Check(o)) {
            const char* v = throw_ifnull(PyBytes_AsString(o));
            return core::cfg::Sections::parse(v);
        }
        if (PyUnicode_Check(o)) {
            const char* v = throw_ifnull(PyUnicode_AsUTF8(o));
            return core::cfg::Sections::parse(v);
        }
        PyErr_SetString(PyExc_TypeError, "value must be an instance of str, or bytes");
        throw PythonException();
    } ARKI_CATCH_RETHROW_PYTHON
}


namespace {

struct PythonLineReader : public core::LineReader
{
    PyObject* iter;

    PythonLineReader(PyObject* obj)
        : iter(throw_ifnull(PyObject_GetIter(obj)))
    {
    }
    virtual ~PythonLineReader()
    {
        if (iter) Py_DECREF(iter);
    }

    bool getline(std::string& line) override
    {
        if (!iter)
            return false;

        pyo_unique_ptr item(PyIter_Next(iter));
        if (!item)
        {
            if (PyErr_Occurred())
                throw PythonException();
            Py_DECREF(iter);
            iter = nullptr;
            return false;
        }

        line = from_python<std::string>(item);
        while (!line.empty())
        {
            char tail = line[line.size() - 1];
            if (tail == '\n' || tail == '\r')
                line.resize(line.size() - 1);
            else
                break;
        }
        return true;
    }

    bool eof() const override
    {
        return iter == nullptr;
    }
};

}

std::unique_ptr<core::LineReader> linereader_from_python(PyObject* o)
{
    return std::unique_ptr<core::LineReader>(new PythonLineReader(o));
}

namespace {

struct PyDestFunc
{
    PyObject* callable;

    PyDestFunc(PyObject* callable)
        : callable(callable)
    {
        Py_XINCREF(callable);
    }

    PyDestFunc(const PyDestFunc& o)
        : callable(o.callable)
    {
        Py_XINCREF(callable);
    }

    PyDestFunc(PyDestFunc&& o)
        : callable(o.callable)
    {
        o.callable = nullptr;
    }

    ~PyDestFunc()
    {
        Py_XDECREF(callable);
    }

    PyDestFunc& operator=(const PyDestFunc& o)
    {
        Py_XINCREF(o.callable);
        Py_XDECREF(callable);
        callable = o.callable;
        return *this;
    }

    PyDestFunc& operator=(PyDestFunc&& o)
    {
        if (this == &o)
            return *this;

        Py_XDECREF(callable);
        callable = o.callable;
        o.callable = nullptr;
        return *this;
    }

    bool operator()(std::unique_ptr<Metadata> md)
    {
        AcquireGIL gil;
        // call arg_on_metadata
        py_unique_ptr<arkipy_Metadata> pymd(metadata_create(std::move(md)));
        pyo_unique_ptr args(PyTuple_Pack(1, pymd.get()));
        if (!args) throw PythonException();
        pyo_unique_ptr res(PyObject_CallObject(callable, args));
        if (!res) throw PythonException();
        // Continue if the callback returns None or True
        if (res == Py_None) return true;
        int cont = PyObject_IsTrue(res);
        if (cont == -1) throw PythonException();
        return cont == 1;
    }
};

}

arki::metadata_dest_func dest_func_from_python(PyObject* o)
{
    if (PyCallable_Check(o))
    {
        return PyDestFunc(o);
    }
    PyErr_SetString(PyExc_TypeError, "value must be a callable");
    throw PythonException();
}

arki::Matcher matcher_from_python(PyObject* o)
{
    if (o == Py_None)
        return arki::Matcher();

    if (arkipy_Matcher_Check(o))
        return ((arkipy_Matcher*)o)->matcher;

    return arki::Matcher::parse(from_python<std::string>(o));
}

int common_init()
{
    arki::init();
#if 0
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
#endif

    return 0;
}

}
}
