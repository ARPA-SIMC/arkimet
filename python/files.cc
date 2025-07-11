#include "files.h"
#include "arki/runtime.h"
#include "arki/stream/abstract.h"
#include "common.h"
#include "utils/values.h"
#include <string>

namespace {

using namespace arki::python;

static std::filesystem::path get_fd_name(PyObject* o)
{
    // First try reading o.name
    pyo_unique_ptr o_name(PyObject_GetAttrString(o, "name"));
    if (o_name)
    {
        if (!PyUnicode_Check(o_name))
            o_name.reset(throw_ifnull(PyObject_Str(o_name)));
        return from_python<std::filesystem::path>(o_name);
    }
    PyErr_Clear();

    // Then try repr
    pyo_unique_ptr o_str(PyObject_Str(o));
    if (o_str)
        return from_python<std::string>(o_str);

    throw PythonException();
}

class PythonStreamOutput
    : public arki::stream::AbstractStreamOutput<arki::stream::LinuxBackend>
{
protected:
    PyObject* o;

public:
    PythonStreamOutput(PyObject* o) : o(o) { Py_INCREF(o); }
    PythonStreamOutput(const PythonStreamOutput&)            = delete;
    PythonStreamOutput(PythonStreamOutput&&)                 = delete;
    PythonStreamOutput& operator=(const PythonStreamOutput&) = delete;
    PythonStreamOutput& operator=(PythonStreamOutput&&)      = delete;
    ~PythonStreamOutput() { Py_DECREF(o); }

    std::string name() const override
    {
        AcquireGIL gil;
        return get_fd_name(o);
    }

    std::filesystem::path path() const override
    {
        AcquireGIL gil;
        return get_fd_name(o);
    }
};

class PythonTextStreamOutput : public PythonStreamOutput
{
protected:
    arki::stream::SendResult _write_output_buffer(const void* data,
                                                  size_t size) override
    {
        {
            AcquireGIL gil;
            pyo_unique_ptr res(throw_ifnull(PyObject_CallMethod(
                o, "write", "s#", (const char*)data, (Py_ssize_t)size)));
        }
        return arki::stream::SendResult();
    }

    arki::stream::SendResult _write_output_line(const void* data,
                                                size_t size) override
    {
        {
            AcquireGIL gil;
            pyo_unique_ptr res(throw_ifnull(PyObject_CallMethod(
                o, "write", "s#", (const char*)data, (Py_ssize_t)size)));
            res = throw_ifnull(PyObject_CallMethod(o, "write", "C", (int)'\n'));
        }
        return arki::stream::SendResult();
    }

public:
    using PythonStreamOutput::PythonStreamOutput;
};

class PythonBinaryStreamOutput : public PythonStreamOutput
{
protected:
    arki::stream::SendResult _write_output_buffer(const void* data,
                                                  size_t size) override
    {
        {
            AcquireGIL gil;
            pyo_unique_ptr res(throw_ifnull(PyObject_CallMethod(
                o, "write", "y#", (const char*)data, (Py_ssize_t)size)));
        }
        return arki::stream::SendResult();
    }

    arki::stream::SendResult _write_output_line(const void* data,
                                                size_t size) override
    {
        {
            AcquireGIL gil;
            pyo_unique_ptr res(throw_ifnull(PyObject_CallMethod(
                o, "write", "y#", (const char*)data, (Py_ssize_t)size)));
            res = throw_ifnull(PyObject_CallMethod(o, "write", "c", (int)'\n'));
        }
        return arki::stream::SendResult();
    }

public:
    using PythonStreamOutput::PythonStreamOutput;
};

template <typename Base> struct PyFile : public Base
{
    PyObject* o;

    PyFile(PyObject* o) : o(o) { Py_INCREF(o); }
    PyFile(const PyFile&)            = delete;
    PyFile(PyFile&&)                 = delete;
    PyFile& operator=(const PyFile&) = delete;
    PyFile& operator=(PyFile&&)      = delete;
    ~PyFile() { Py_DECREF(o); }

    std::string name() const override
    {
        AcquireGIL gil;
        return get_fd_name(o);
    }

    std::filesystem::path path() const override
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
        pyo_unique_ptr res(throw_ifnull(
            PyObject_CallMethod(o, "read", "n", (Py_ssize_t)size)));
        if (res == Py_None)
            return 0;

        Py_ssize_t res_len;
        const char* res_buffer =
            throw_ifnull(PyUnicode_AsUTF8AndSize(res, &res_len));

        if ((size_t)res_len > size)
        {
            PyErr_Format(PyExc_RuntimeError,
                         "asked to read %zu characters, and got %zi bytes that "
                         "do not fit in the output buffer",
                         size, res_len);
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
        pyo_unique_ptr res(throw_ifnull(
            PyObject_CallMethod(o, "read", "n", (Py_ssize_t)size)));
        if (res == Py_None)
            return 0;

        char* res_buffer;
        Py_ssize_t res_len;
        if (PyBytes_AsStringAndSize(res, &res_buffer, &res_len) == -1)
            throw PythonException();

        if ((size_t)res_len > size)
        {
            PyErr_Format(PyExc_RuntimeError,
                         "asked to read %zu bytes, and got %zi bytes instead",
                         size, res_len);
            throw PythonException();
        }

        memcpy(dest, res_buffer, res_len);
        return res_len;
    }
};

} // namespace

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
        fd                   = new core::File(pathname, O_RDONLY);
        return;
    }

    // Try calling fileno
    pyo_unique_ptr fileno(PyObject_CallMethod(o, "fileno", nullptr));
    if (fileno)
    {
        // It would be nice to give up and fallback to AbstractInputFile if
        // there are bytes in the read buffer of o, but there seems to be no
        // way to know
        fd = new core::NamedFileDescriptor(int_from_python(fileno),
                                           get_fd_name(o));
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
        fd                   = new core::File(pathname, O_RDONLY);
        return;
    }

    // Try calling fileno
    pyo_unique_ptr fileno(PyObject_CallMethod(o, "fileno", nullptr));
    if (fileno)
    {
        // It would be nice to give up and fallback to AbstractInputFile if
        // there are bytes in the read buffer of o, but there seems to be no
        // way to know
        fd = new core::NamedFileDescriptor(int_from_python(fileno),
                                           get_fd_name(o));
        return;
    }

    PyErr_Clear();

    // Fall back on calling o.read()
    abstract = new PyAbstractBinaryInputFile(o);
}

std::unique_ptr<StreamOutput> textio_stream_output(PyObject* o)
{
    const auto& cfg = arki::Config::get();

    // If it is already an int handle, use it
    if (PyLong_Check(o))
        return StreamOutput::create(std::make_shared<core::NamedFileDescriptor>(
                                        int_from_python(o), get_fd_name(o)),
                                    cfg.io_timeout_ms);

    // If it is a string, take it as a file name
    if (PyUnicode_Check(o))
        return StreamOutput::create(
            std::make_shared<core::File>(from_python<std::string>(o),
                                         O_WRONLY | O_CREAT | O_TRUNC, 0666),
            cfg.io_timeout_ms);

    // Try calling fileno
    pyo_unique_ptr fileno(PyObject_CallMethod(o, "fileno", nullptr));
    if (fileno)
    {
        // It would be nice to give up and fallback to AbstractInputFile if
        // there are bytes in the read buffer of o, but there seems to be no
        // way to know
        return StreamOutput::create(
            std::make_shared<core::NamedFileDescriptor>(int_from_python(fileno),
                                                        get_fd_name(o)),
            cfg.io_timeout_ms);
    }

    PyErr_Clear();

    // Fall back on calling o.write()
    return std::unique_ptr<StreamOutput>(new PythonTextStreamOutput(o));
}

std::unique_ptr<StreamOutput> binaryio_stream_output(PyObject* o)
{
    const auto& cfg = arki::Config::get();

    // If it is already an int handle, use it
    if (PyLong_Check(o))
        return StreamOutput::create(std::make_shared<core::NamedFileDescriptor>(
                                        int_from_python(o), get_fd_name(o)),
                                    cfg.io_timeout_ms);

    // If it is a string, take it as a file name
    if (PyUnicode_Check(o))
        return StreamOutput::create(
            std::make_shared<core::File>(from_python<std::string>(o),
                                         O_WRONLY | O_CREAT | O_TRUNC, 0666),
            cfg.io_timeout_ms);

    // Try calling fileno
    pyo_unique_ptr fileno(PyObject_CallMethod(o, "fileno", nullptr));
    if (fileno)
    {
        // It would be nice to give up and fallback to AbstractInputFile if
        // there are bytes in the read buffer of o, but there seems to be no
        // way to know
        return StreamOutput::create(
            std::make_shared<core::NamedFileDescriptor>(int_from_python(fileno),
                                                        get_fd_name(o)),
            cfg.io_timeout_ms);
    }

    PyErr_Clear();

    // Fall back on calling o.write()
    return std::unique_ptr<StreamOutput>(new PythonBinaryStreamOutput(o));
}

} // namespace python
} // namespace arki
