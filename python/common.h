#ifndef ARKI_PYTHON_COMMON_H
#define ARKI_PYTHON_COMMON_H

#include <Python.h>
#include <string>

namespace arki {
namespace python {

/// Given a generic exception, set the Python error indicator appropriately.
void set_std_exception(const std::exception& e);


#define ARKI_CATCH_RETURN_PYO \
      catch (std::exception& se) { \
        set_std_exception(se); return nullptr; \
    }

#define ARKI_CATCH_RETURN_INT \
      catch (std::exception& se) { \
        set_std_exception(se); return -1; \
    }

#if 0
extern wrpy_c_api* wrpy;

/**
 * unique_ptr-like object that contains PyObject pointers, and that calls
 * Py_DECREF on destruction.
 */
template<typename Obj>
class py_unique_ptr
{
protected:
    Obj* ptr;

public:
    py_unique_ptr(Obj* o) : ptr(o) {}
    py_unique_ptr(const py_unique_ptr&) = delete;
    py_unique_ptr(py_unique_ptr&& o) : ptr(o.ptr) { o.ptr = nullptr; }
    ~py_unique_ptr() { Py_XDECREF(ptr); }
    py_unique_ptr& operator=(const py_unique_ptr&) = delete;
    py_unique_ptr& operator=(py_unique_ptr&& o)
    {
        if (this == &o) return *this;
        Py_XDECREF(ptr);
        ptr = o.ptr;
        o.ptr = nullptr;
        return *this;
    }

    /// Release the reference without calling Py_DECREF
    Obj* release()
    {
        Obj* res = ptr;
        ptr = nullptr;
        return res;
    }

    /// Use it as a Obj
    operator Obj*() { return ptr; }

    /// Get the pointer (useful for passing to Py_BuildValue)
    Obj* get() { return ptr; }

    /// Check if ptr is not nullptr
    operator bool() const { return ptr; }
};

typedef py_unique_ptr<PyObject> pyo_unique_ptr;

/// Convert a Datetime to a python datetime object
PyObject* datetime_to_python(const Datetime& dt);

/// Convert a python datetime object to a Datetime
int datetime_from_python(PyObject* dt, Datetime& out);

/// Convert a sequence of two python datetime objects to a DatetimeRange
int datetimerange_from_python(PyObject* dt, DatetimeRange& out);

/// Convert a Level to a python 4-tuple
PyObject* level_to_python(const Level& lev);

/// Convert a 4-tuple to a Level
int level_from_python(PyObject* o, Level& out);

/// Convert a Trange to a python 3-tuple
PyObject* trange_to_python(const Trange& tr);

/// Convert a 3-tuple to a Trange
int trange_from_python(PyObject* o, Trange& out);

/// Call repr() on \a o, and return the result in \a out
int object_repr(PyObject* o, std::string& out);

/**
 * call o.fileno() and return its result.
 *
 * In case of AttributeError and IOError (parent of UnsupportedOperation, not
 * available from C), it clear the error indicator.
 *
 * Returns -1 if fileno() was not available or some other exception happened.
 * Use PyErr_Occurred to tell between the two.
 */
int file_get_fileno(PyObject* o);

/**
 * call o.data() and return its result, both as a PyObject and as a buffer.
 *
 * The data returned in buf and len will be valid as long as the returned
 * object stays valid.
 */
PyObject* file_get_data(PyObject* o, char*&buf, Py_ssize_t& len);
#endif

/**
 * Convert a python string, bytes or unicode to an utf8 string.
 *
 * Return 0 on success, -1 on failure
 */
int string_from_python(PyObject* o, std::string& out);

/**
 * Initialize the python bits to use used by the common functions.
 *
 * This can be called multiple times and will execute only once.
 */
int common_init();

}
}
#endif
