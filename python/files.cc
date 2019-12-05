#include "files.h"
#include "utils/values.h"
#include "common.h"
#include <string>

namespace {

using namespace arki::python;

static std::string get_fd_name(PyObject* o)
{
    // First try reading o.name
    pyo_unique_ptr o_name(PyObject_GetAttrString(o, "name"));
    if (o_name)
    {
        if (!PyUnicode_Check(o_name))
            o_name.reset(throw_ifnull(PyObject_Str(o_name)));
        return from_python<std::string>(o_name);
    }
    PyErr_Clear();

    // Then try repr
    pyo_unique_ptr o_str(PyObject_Str(o));
    if (o_str)
        return from_python<std::string>(o_str);

    throw PythonException();
}


template<typename Base>
struct PyFile : public Base
{
    PyObject* o;

    PyFile(PyObject* o)
        : o(o)
    {
        Py_INCREF(o);
    }
    PyFile(const PyFile&) = delete;
    PyFile(PyFile&&) = delete;
    PyFile& operator=(const PyFile&) = delete;
    PyFile& operator=(PyFile&&) = delete;
    ~PyFile()
    {
        Py_DECREF(o);
    }

    std::string name() const override
    {
        AcquireGIL gil;
        return get_fd_name(o);
    }
};


struct PyAbstractTextInputFile : public PyFile<arki::core::AbstractInputFile>
{
    using PyFile::PyFile;

    size_t read(void* dest, size_t size) override
    {
        AcquireGIL gil;
        pyo_unique_ptr res(throw_ifnull(PyObject_CallMethod(o, "read", "n", (Py_ssize_t)size)));
        if (res == Py_None)
            return 0;

        Py_ssize_t res_len;
        const char* res_buffer = throw_ifnull(PyUnicode_AsUTF8AndSize(res, &res_len));

        if ((size_t)res_len > size)
        {
            PyErr_Format(PyExc_RuntimeError, "asked to read %zu characters, and got %zi bytes that do not fit in the output buffer", size, res_len);
            throw PythonException();
        }

        memcpy(dest, res_buffer, res_len);
        return res_len;
    }
};

struct PyAbstractBinaryInputFile : public PyFile<arki::core::AbstractInputFile>
{
    using PyFile::PyFile;

    size_t read(void* dest, size_t size) override
    {
        AcquireGIL gil;
        pyo_unique_ptr res(throw_ifnull(PyObject_CallMethod(o, "read", "n", (Py_ssize_t)size)));
        if (res == Py_None)
            return 0;

        char* res_buffer;
        Py_ssize_t res_len;
        if (PyBytes_AsStringAndSize(res, &res_buffer, &res_len) == -1)
            throw PythonException();

        if ((size_t)res_len > size)
        {
            PyErr_Format(PyExc_RuntimeError, "asked to read %zu bytes, and got %zi bytes instead", size, res_len);
            throw PythonException();
        }

        memcpy(dest, res_buffer, res_len);
        return res_len;
    }
};

struct PyAbstractTextOutputFile : public PyFile<arki::core::AbstractOutputFile>
{
    using PyFile::PyFile;

    void write(const void* data, size_t size) override
    {
        AcquireGIL gil;
        pyo_unique_ptr res(throw_ifnull(PyObject_CallMethod(o, "write", "s#", (const char*)data, (Py_ssize_t)size)));
    }
};

struct PyAbstractBinaryOutputFile : public PyFile<arki::core::AbstractOutputFile>
{
    using PyFile::PyFile;

    void write(const void* data, size_t size) override
    {
        AcquireGIL gil;
        pyo_unique_ptr res(throw_ifnull(PyObject_CallMethod(o, "write", "y#", (const char*)data, (Py_ssize_t)size)));
    }
};

}


namespace arki {
namespace python {

TextInputFile::TextInputFile(PyObject* o)
{
    // If it is already an int handle, use it
    if (PyLong_Check(o))
    {
        fd = new core::NamedFileDescriptor(int_from_python(o), get_fd_name(o));
        return;
    }

    // If it is a string, take it as a file name
    if (PyUnicode_Check(o))
    {
        std::string pathname = from_python<std::string>(o);
        fd = new core::File(pathname, O_RDONLY);
        return;
    }

    // Try calling fileno
    pyo_unique_ptr fileno(PyObject_CallMethod(o, "fileno", nullptr));
    if (fileno)
    {
        // It would be nice to give up and fallback to AbstractInputFile if
        // there are bytes in the read buffer of o, but there seems to be no
        // way to know
        fd = new core::NamedFileDescriptor(int_from_python(fileno), get_fd_name(o));
        return;
    }

    PyErr_Clear();

    // Fall back on calling o.read()
    abstract = new PyAbstractTextInputFile(o);
}


BinaryInputFile::BinaryInputFile(PyObject* o)
{
    // If it is already an int handle, use it
    if (PyLong_Check(o))
    {
        fd = new core::NamedFileDescriptor(int_from_python(o), get_fd_name(o));
        return;
    }

    // If it is a string, take it as a file name
    if (PyUnicode_Check(o))
    {
        std::string pathname = from_python<std::string>(o);
        fd = new core::File(pathname, O_RDONLY);
        return;
    }

    // Try calling fileno
    pyo_unique_ptr fileno(PyObject_CallMethod(o, "fileno", nullptr));
    if (fileno)
    {
        // It would be nice to give up and fallback to AbstractInputFile if
        // there are bytes in the read buffer of o, but there seems to be no
        // way to know
        fd = new core::NamedFileDescriptor(int_from_python(fileno), get_fd_name(o));
        return;
    }

    PyErr_Clear();

    // Fall back on calling o.read()
    abstract = new PyAbstractBinaryInputFile(o);
}


TextOutputFile::TextOutputFile(PyObject* o)
{
    // If it is already an int handle, use it
    if (PyLong_Check(o))
    {
        fd = new core::NamedFileDescriptor(int_from_python(o), get_fd_name(o));
        return;
    }

    // If it is a string, take it as a file name
    if (PyUnicode_Check(o))
    {
        std::string pathname = from_python<std::string>(o);
        fd = new core::File(pathname, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        return;
    }

    // Try calling fileno
    pyo_unique_ptr fileno(PyObject_CallMethod(o, "fileno", nullptr));
    if (fileno)
    {
        // It would be nice to give up and fallback to AbstractInputFile if
        // there are bytes in the read buffer of o, but there seems to be no
        // way to know
        fd = new core::NamedFileDescriptor(int_from_python(fileno), get_fd_name(o));
        return;
    }

    PyErr_Clear();

    // Fall back on calling o.write()
    abstract = new PyAbstractTextOutputFile(o);
}


BinaryOutputFile::BinaryOutputFile(PyObject* o)
{
    // If it is already an int handle, use it
    if (PyLong_Check(o))
    {
        fd = new core::NamedFileDescriptor(int_from_python(o), get_fd_name(o));
        return;
    }

    // If it is a string, take it as a file name
    if (PyUnicode_Check(o))
    {
        std::string pathname = from_python<std::string>(o);
        fd = new core::File(pathname, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        return;
    }

    // Try calling fileno
    pyo_unique_ptr fileno(PyObject_CallMethod(o, "fileno", nullptr));
    if (fileno)
    {
        // It would be nice to give up and fallback to AbstractInputFile if
        // there are bytes in the read buffer of o, but there seems to be no
        // way to know
        fd = new core::NamedFileDescriptor(int_from_python(fileno), get_fd_name(o));
        return;
    }

    PyErr_Clear();

    // Fall back on calling o.write()
    abstract = new PyAbstractBinaryOutputFile(o);
}

}
}
