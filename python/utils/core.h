#ifndef ARKI_PYTHON_CORE_H
#define ARKI_PYTHON_CORE_H

#include <Python.h>
#include <stdexcept>

namespace arki {
namespace python {

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
    py_unique_ptr() : ptr(nullptr) {}
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

    void incref() { Py_XINCREF(ptr); }
    void decref() { Py_XDECREF(ptr); }

    void clear()
    {
        Py_XDECREF(ptr);
        ptr = nullptr;
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

    /// Use it as a Obj
    Obj* operator->() { return ptr; }

    /// Get the pointer (useful for passing to Py_BuildValue)
    Obj* get() { return ptr; }

    /// Check if ptr is not nullptr
    operator bool() const { return ptr; }
};

typedef py_unique_ptr<PyObject> pyo_unique_ptr;


/**
 * Release the GIL during the lifetime of this object;
 */
struct ReleaseGIL
{
    PyThreadState *_save = nullptr;

    ReleaseGIL()
    {
        _save = PyEval_SaveThread();
    }

    ~ReleaseGIL()
    {
        lock();
    }

    void lock()
    {
        if (!_save) return;
        PyEval_RestoreThread(_save);
        _save = nullptr;
    }
};


/**
 * Exception raised when a python function returns an error with an exception
 * set.
 *
 * When catching this exception, python exception information is already set,
 * so the only thing to do is to return the appropriate error to the python
 * caller.
 *
 * This exception carries to information, because it is all set in the python
 * exception information.
 */
struct PythonException : public std::exception {};


/**
 * Throw PythonException if the given pointer is nullptr.
 *
 * This can be used to wrap Python API invocations, throwing PythonException if
 * the API call returned an error.
 */
template<typename T>
inline T* throw_ifnull(T* o)
{
    if (!o) throw PythonException();
    return o;
}

}

#endif
