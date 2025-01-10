#ifndef ARKI_PYTHON_COMMON_H
#define ARKI_PYTHON_COMMON_H

#include "utils/core.h"
#include "utils/values.h"
#include <string>
#include <vector>
#include <memory>
#include <arki/defs.h>
#include <arki/core/fwd.h>

namespace arki {
namespace python {

/// Given a generic exception, set the Python error indicator appropriately.
void set_std_exception(const std::exception& e);


#define ARKI_CATCH_RETURN_PYO \
      catch (arki::python::PythonException&) { \
        return nullptr; \
    } catch (std::invalid_argument& e) { \
        PyErr_SetString(PyExc_ValueError, e.what()); return nullptr; \
    } catch (std::exception& se) { \
        arki::python::set_std_exception(se); return nullptr; \
    }

#define ARKI_CATCH_RETURN_INT \
      catch (arki::python::PythonException&) { \
        return -1; \
    } catch (std::invalid_argument& e) { \
        PyErr_SetString(PyExc_ValueError, e.what()); return -1; \
    } catch (std::exception& se) { \
        arki::python::set_std_exception(se); return -1; \
    }

#define ARKI_CATCH_RETHROW_PYTHON \
      catch (arki::python::PythonException&) { \
        throw; \
    } catch (std::invalid_argument& e) { \
        PyErr_SetString(PyExc_ValueError, e.what()); throw arki::python::PythonException(); \
    } catch (std::exception& se) { \
        arki::python::set_std_exception(se); throw arki::python::PythonException(); \
    }


// Define Py_RETURN_RICHCOMPARE for compatibility for pre-3.7 python versions
#ifndef Py_RETURN_RICHCOMPARE
#define Py_RETURN_RICHCOMPARE(val1, val2, op)                               \
    do {                                                                    \
        switch (op) {                                                       \
        case Py_EQ: if ((val1) == (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_NE: if ((val1) != (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_LT: if ((val1) < (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;   \
        case Py_GT: if ((val1) > (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;   \
        case Py_LE: if ((val1) <= (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_GE: if ((val1) >= (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        default:                                                            \
            abort();                                                        \
        }                                                                   \
    } while (0)
#endif

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 7
PyObject *ArkiPyImport_GetModule(PyObject *name);
#else
#define ArkiPyImport_GetModule PyImport_GetModule
#endif

/**
 * Create a LineReader to read from any python object that can iterate strings
 */
std::unique_ptr<core::LineReader> linereader_from_python(PyObject* o);


/**
 * Base class for python objects what wrap a std::shared_ptr
 */
template<typename Class>
struct SharedPtrWrapper
{
    PyObject_HEAD
    std::shared_ptr<Class> ptr;

    typedef Class value_type;

    PyObject* python__exit__()
    {
        try {
            ptr.reset();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};


/// Convert a Python object to a DataFormat
DataFormat dataformat_from_python(PyObject* o);
template<> inline DataFormat from_python<DataFormat>(PyObject* o) { return dataformat_from_python(o); }

/// Convert a DataFormat to a Python object
PyObject* dataformat_to_python(DataFormat val);
inline PyObject* to_python(DataFormat val) { return dataformat_to_python(val); }



/**
 * Initialize the python bits to use used by the common functions.
 *
 * This can be called multiple times and will execute only once.
 */
int common_init();

}
}
#endif
