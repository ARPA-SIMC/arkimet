#include "files.h"
#include "arki/python/common.h"
#include "arki/python/utils/type.h"
#include "arki/python/utils/values.h"
#include "arki/runtime.h"
#include "arki/stream/abstract.h"
#include "arki/utils/string.h"
#include <filesystem>
#include <string>

namespace {
struct FileProxy;
}

extern "C" {
FileProxy* file_proxy_def = nullptr;
PyTypeObject* arkipy_File_Type;
}

namespace {

using namespace arki::python;
using namespace arki::utils;

struct file_name : public Getter<file_name, arkipy_File>
{
    constexpr static const char* name = "name";
    constexpr static const char* doc  = "File name (str).";
    constexpr static void* closure    = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try
        {
            return to_python(self->fd.path().native());
        }
        ARKI_CATCH_RETURN_PYO;
    }
};

struct fileno : public MethNoargs<fileno, arkipy_File>
{
    constexpr static const char* name      = "fileno";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "int";
    constexpr static const char* summary =
        "return the underlying system file descriptor";

    static PyObject* run(Impl* self)
    {
        try
        {
            return int_to_python(self->fd);
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct file_read : public MethVarargs<file_read, arkipy_File>
{
    constexpr static const char* name      = "read";
    constexpr static const char* signature = "size=-1, /";
    constexpr static const char* returns   = "bytes";
    constexpr static const char* summary   = "read from the file.";

    static PyObject* read_all(Impl* self)
    {
        const size_t buffer_size = 4096 * 16;
        size_t total_read        = 0;

        // Create an empty bytes object
        PyObject* res = PyBytes_FromStringAndSize(NULL, 0);
        if (!res)
            return nullptr;

        while (true)
        {
            // Resize the bytes object
            if (_PyBytes_Resize(&res, total_read + buffer_size) == -1)
                return nullptr;

            // Read new data into the bytes object
            size_t bytes_read = 0;
            try
            {
                bytes_read = self->fd.read(PyBytes_AsString(res) + total_read,
                                           buffer_size);
            }
            catch (std::exception& exc)
            {
                Py_XDECREF(res);
                arki::python::set_std_exception(exc);
                return nullptr;
            }

            // Stop on EOF
            if (bytes_read == 0)
                break;

            total_read += bytes_read;
        }

        // Resize the bytes object to the actual amount of bytes read
        if (_PyBytes_Resize(&res, total_read) == -1)
            return nullptr;
        return res;
    }

    static PyObject* read_some(Impl* self, size_t size)
    {
        PyObject* res = PyBytes_FromStringAndSize(NULL, size);
        if (!res)
            return nullptr;

        size_t bytes_read = 0;
        try
        {
            bytes_read = self->fd.read(PyBytes_AsString(res), size);
        }
        catch (std::exception& exc)
        {
            Py_XDECREF(res);
            arki::python::set_std_exception(exc);
            return nullptr;
        }

        if (_PyBytes_Resize(&res, bytes_read) == -1)
            return nullptr;
        return res;
    }

    static PyObject* run(Impl* self, PyObject* args)
    {
        try
        {
            Py_ssize_t args_size = PyTuple_Size(args);
            if (args_size == -1)
                return nullptr;

            if (args_size == 0)
                return read_all(self);

            // PyTuple_GetItem returns a borrowed reference, so we do not need
            // to wrap it into a pyo_unique_ptr
            PyObject* py_size = throw_ifnull(PyTuple_GetItem(args, 0));
            int size          = int_from_python(py_size);
            if (size < 0)
                return read_all(self);
            else
                return read_some(self, size);
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct FileProxy : public Type<FileProxy, arkipy_File>
{
    constexpr static const char* name      = "File";
    constexpr static const char* qual_name = "arkimet.File";
    constexpr static const char* doc       = R"(
File-like object to read data from files opened by the Arkimet library.
)";
    GetSetters<file_name> getsetters;
    Methods<fileno, file_read> methods;

    static void _dealloc(Impl* self)
    {
        self->fd.~NamedFileDescriptor();
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        try
        {
            return to_python(self->fd.path());
        }
        ARKI_CATCH_RETURN_PYO
    }

    static PyObject* _repr(Impl* self)
    {
        try
        {
            std::string res =
                "arkimet.File(fd=" + std::to_string((int)self->fd) +
                ", name=" + str::encode_cstring(self->fd.path().native()) + ")";
            return PyUnicode_FromStringAndSize(res.data(), res.size());
        }
        ARKI_CATCH_RETURN_PYO
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"fd", "file", nullptr};
        int arg_fd;
        const char* arg_file;
        Py_ssize_t arg_file_size;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "is#",
                                         const_cast<char**>(kwlist), &arg_fd,
                                         &arg_file, &arg_file_size))
            return -1;

        try
        {
            sys::NamedFileDescriptor fd(
                arg_fd, std::string(arg_file, (size_t)arg_file_size));

            new (&(self->fd)) arki::utils::sys::NamedFileDescriptor(fd);
            return 0;
        }
        ARKI_CATCH_RETURN_INT
    }
};

static std::filesystem::path
get_fd_name(PyObject* o,
            const std::filesystem::path& name_hint = std::filesystem::path())
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

    if (!name_hint.empty())
        return name_hint;

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
    std::filesystem::path name_hint;

    PyFile(PyObject* o, const std::filesystem::path& name_hint)
        : o(o), name_hint(name_hint)
    {
        Py_INCREF(o);
    }
    PyFile(const PyFile&)            = delete;
    PyFile(PyFile&&)                 = delete;
    PyFile& operator=(const PyFile&) = delete;
    PyFile& operator=(PyFile&&)      = delete;
    ~PyFile() { Py_DECREF(o); }

    std::string name() const override
    {
        AcquireGIL gil;
        return get_fd_name(o, name_hint);
    }

    std::filesystem::path path() const override
    {
        AcquireGIL gil;
        return get_fd_name(o, name_hint);
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

TextInputFile::TextInputFile(PyObject* o,
                             const std::filesystem::path& name_hint)
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
        // It would be nice to give up and fallback to AbstractInputFile
        // if there are bytes in the read buffer of o, but there seems
        // to be no way to know
        fd = new core::NamedFileDescriptor(int_from_python(fileno),
                                           get_fd_name(o));
        return;
    }

    PyErr_Clear();

    // Fall back on calling o.read()
    abstract = new PyAbstractTextInputFile(o, name_hint);
}

BinaryInputFile::BinaryInputFile(PyObject* o,
                                 const std::filesystem::path& name_hint)
{
    // If it is already an int handle, use it
    if (PyLong_Check(o))
    {
        fd = new core::NamedFileDescriptor(int_from_python(o),
                                           get_fd_name(o, name_hint));
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
        // It would be nice to give up and fallback to AbstractInputFile
        // if there are bytes in the read buffer of o, but there seems
        // to be no way to know
        fd = new core::NamedFileDescriptor(int_from_python(fileno),
                                           get_fd_name(o, name_hint));
        return;
    }

    PyErr_Clear();

    // Fall back on calling o.read()
    abstract = new PyAbstractBinaryInputFile(o, name_hint);
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
        // It would be nice to give up and fallback to AbstractInputFile
        // if there are bytes in the read buffer of o, but there seems
        // to be no way to know
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
        // It would be nice to give up and fallback to AbstractInputFile
        // if there are bytes in the read buffer of o, but there seems
        // to be no way to know
        return StreamOutput::create(
            std::make_shared<core::NamedFileDescriptor>(int_from_python(fileno),
                                                        get_fd_name(o)),
            cfg.io_timeout_ms);
    }

    PyErr_Clear();

    // Fall back on calling o.write()
    return std::unique_ptr<StreamOutput>(new PythonBinaryStreamOutput(o));
}

PyObject* file_to_python(arki::utils::sys::NamedFileDescriptor& fd)
{
    py_unique_ptr<arkipy_File> res(
        throw_ifnull(PyObject_New(arkipy_File, arkipy_File_Type)));
    new (&(res->fd)) arki::utils::sys::NamedFileDescriptor(fd);
    return (PyObject*)res.release();
}

void register_files(PyObject* m)
{
    file_proxy_def = new FileProxy;
    file_proxy_def->define(arkipy_File_Type, m);
}

} // namespace python
} // namespace arki
