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

using namespace std;

extern "C" {

#if Py_MAJOR_VERSION == 3 && Py_MINOR_VERSION < 7
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

}

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



int object_repr(PyObject* o, std::string& out)
{
    pyo_unique_ptr repr(PyObject_Repr(o));
    if (!repr) return -1;

    out = string_from_python(repr);
    return 0;
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
