#include "common.h"
#include "cfg.h"
#include "metadata.h"
#include "matcher.h"
#include "utils/values.h"
#include "arki/libconfig.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/runtime.h"
#include <datetime.h>

using namespace std;

namespace arki {
namespace python {

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 7
PyObject *ArkiPyImport_GetModule(PyObject *name)
{
    PyObject *m;
    PyObject *modules = PyImport_GetModuleDict();
    if (modules == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "unable to get sys.modules");
        return NULL;
    }
    Py_INCREF(modules);
    if (PyDict_CheckExact(modules)) {
        m = PyDict_GetItemWithError(modules, name);  /* borrowed */
        Py_XINCREF(m);
    }
    else {
        m = PyObject_GetItem(modules, name);
        if (m == NULL && PyErr_ExceptionMatches(PyExc_KeyError)) {
            PyErr_Clear();
        }
    }
    Py_DECREF(modules);
    return m;
}
#endif

void set_std_exception(const std::exception& e)
{
    PyErr_SetString(PyExc_RuntimeError, e.what());
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
            fd_eof = true;
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
};

}

std::unique_ptr<core::LineReader> linereader_from_python(PyObject* o)
{
    return std::unique_ptr<core::LineReader>(new PythonLineReader(o));
}

DataFormat dataformat_from_python(PyObject* o)
{
    return format_from_string(string_from_python(o));
}

PyObject* dataformat_to_python(DataFormat val)
{
    return string_to_python(format_name(val));
}

static int get_attr_int(PyObject* o, const char* name)
{
  pyo_unique_ptr res(throw_ifnull(PyObject_GetAttrString(o, name)));
  return from_python<int>(res);
}

core::Time core_time_from_python(PyObject* o)
{
    if (o == NULL || o == Py_None)
        return core::Time();

    if (PyDateTime_Check(0))
    {
        PyDateTime_DateTime* dt = reinterpret_cast<PyDateTime_DateTime*>(o);
        return core::Time(
                PyDateTime_GET_YEAR(dt), PyDateTime_GET_MONTH(dt), PyDateTime_GET_DAY(dt),
                PyDateTime_DATE_GET_HOUR(dt), PyDateTime_DATE_GET_MINUTE(dt), PyDateTime_DATE_GET_SECOND(dt));
    }
  
    // Fall back to duck typing, to catch creative cases such as
    // cftime.datetime instances. See https://unidata.github.io/cftime/api.html
    return core::Time(
            get_attr_int(o, "year"),
            get_attr_int(o, "month"),
            get_attr_int(o, "day"),
            get_attr_int(o, "hour"),
            get_attr_int(o, "minute"),
            get_attr_int(o, "second")
    );

    // PyErr_SetString(PyExc_TypeError, "value must be an instance of datetime.datetime");
    // throw PythonException();

}

PyObject* core_time_to_python(const core::Time& time)
{
    if (!time.isset())
        Py_RETURN_NONE;

    return throw_ifnull(PyDateTime_FromDateAndTime(
                time.ye, time.mo, time.da,
                time.ho, time.mi, time.se, 0));
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

#if 0
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
