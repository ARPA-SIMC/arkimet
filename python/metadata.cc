#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "metadata.h"
#include "common.h"
#include "arki/metadata.h"
#include "arki/core/file.h"
#include "arki/types/source.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_Metadata_Type = nullptr;

}

namespace {

/*
 * Metadata
 */

struct write : public MethKwargs<write, arkipy_Metadata>
{
    constexpr static const char* name = "write";
    constexpr static const char* signature = "file: BytesIO, format: str";
    constexpr static const char* returns = "None";
    constexpr static const char* summary = "Write the metadata to a file";
    constexpr static const char* doc = R"(
Arguments:
  file: the output file. The file needs to be either an integer file or
        socket handle, or a file-like object with a fileno() method
        that returns an integer handle.
  format: "binary", "yaml", or "json". Default: "binary".
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", "format", NULL };
        PyObject* arg_file = Py_None;
        const char* format = nullptr;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|s", (char**)kwlist, &arg_file, &format))
            return nullptr;

        try {
            int fd = file_get_fileno(arg_file);
            if (fd == -1) return nullptr;
            std::string fd_name;
            if (object_repr(arg_file, fd_name) == -1) return nullptr;

            if (!format || strcmp(format, "binary") == 0)
            {
                NamedFileDescriptor out(fd, fd_name);
                self->md->write(out);
            } else if (strcmp(format, "yaml") == 0) {
                PyErr_SetString(PyExc_NotImplementedError, "serializing to YAML is not yet implemented");
                return nullptr;
            } else if (strcmp(format, "json") == 0) {
                PyErr_SetString(PyExc_NotImplementedError, "serializing to JSON is not yet implemented");
                return nullptr;
            } else {
                PyErr_Format(PyExc_ValueError, "Unsupported metadata serializati format: %s", format);
                return nullptr;
            }
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct make_inline : public MethNoargs<make_inline, arkipy_Metadata>
{
    constexpr static const char* name = "make_inline";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "Read the data and inline them in the metadata";

    static PyObject* run(Impl* self)
    {
        try {
            self->md->makeInline();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct make_url : public MethKwargs<make_url, arkipy_Metadata>
{
    constexpr static const char* name = "make_url";
    constexpr static const char* signature = "baseurl: str";
    constexpr static const char* returns = "None";
    constexpr static const char* summary = "Set the data source as URL";
    constexpr static const char* doc = R"(
Arguments:
  baseurl: the base URL that identifies the dataset
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "baseurl", NULL };
        const char* url = nullptr;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s", (char**)kwlist, &url))
            return nullptr;

        try {
            self->md->set_source(types::Source::createURL(self->md->source().format, url));
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct to_python : public MethKwargs<to_python, arkipy_Metadata>
{
    constexpr static const char* name = "to_python";
    constexpr static const char* signature = "type: str=None";
    constexpr static const char* returns = "dict";
    constexpr static const char* summary = "Return the metadata contents in a python dict";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "baseurl", NULL };
        const char* py_type = nullptr;
        Py_ssize_t py_type_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#", (char**)kwlist, &py_type, &py_type_len))
            return nullptr;

        try {
            PythonEmitter e;
            if (py_type)
            {
                arki::types::Code code = arki::types::parseCodeName(std::string(py_type, py_type_len));
                if (code == arki::TYPE_SOURCE)
                {
                    if (!self->md->has_source())
                        Py_RETURN_NONE;
                    else
                        self->md->source().serialise(e);
                } else {
                    const types::Type* item = self->md->get(code);
                    if (!item)
                        Py_RETURN_NONE;
                    item->serialise(e);
                }
            } else {
                self->md->serialise(e);
            }
            return e.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

std::string get_fd_name(PyObject* fd)
{
    pyo_unique_ptr res(throw_ifnull(PyObject_GetAttrString(fd, "name")));
    return from_python<std::string>(res);
}

struct read_bundle : public ClassMethKwargs<read_bundle>
{
    constexpr static const char* name = "read_bundle";
    constexpr static const char* signature = "src: Union[bytes, ByteIO], dest: Callable[[metadata], Optional[bool]], basedir: str=None, pathname: str=None";
    constexpr static const char* returns = "bool";
    constexpr static const char* summary = "Read all metadata from a give file or memory buffer";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "src", "dest", "basedir", "pathname", NULL };
        PyObject* py_src = nullptr;
        PyObject* py_dest = nullptr;
        const char* py_basedir = nullptr;
        Py_ssize_t py_basedir_len;
        const char* py_pathname = nullptr;
        Py_ssize_t py_pathname_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OO|z#z#",
                    (char**)kwlist, &py_src, &py_dest,
                    &py_basedir, &py_basedir_len, &py_pathname, &py_pathname_len))
            return nullptr;

        try {
            metadata_dest_func dest = dest_func_from_python(py_dest);

            bool res;
            if (PyBytes_Check(py_src))
            {
                char* buffer;
                Py_ssize_t length;
                if (PyBytes_AsStringAndSize(py_src, &buffer, &length) == -1)
                    throw PythonException();

                if (py_basedir && py_pathname)
                {
                    arki::metadata::ReadContext ctx(
                            std::string(py_pathname, py_pathname_len),
                            std::string(py_basedir, py_basedir_len));
                    res = arki::Metadata::read_buffer((uint8_t*)buffer, length, ctx, dest);
                } else if (py_basedir) {
                    PyErr_SetString(PyExc_ValueError, "basedir provided without pathname when parsing metadata from a memory buffer");
                    return nullptr;
                } else if (py_pathname) {
                    arki::metadata::ReadContext ctx(std::string(py_pathname, py_pathname_len));
                    res = arki::Metadata::read_buffer((uint8_t*)buffer, length, ctx, dest);
                } else {
                    arki::metadata::ReadContext ctx;
                    res = arki::Metadata::read_buffer((uint8_t*)buffer, length, ctx, dest);
                }
            } else {
                int fd = file_get_fileno(py_src);
                if (fd == -1) return nullptr;

                if (py_basedir && py_pathname)
                {
                    arki::metadata::ReadContext ctx(
                            std::string(py_pathname, py_pathname_len),
                            std::string(py_basedir, py_basedir_len));
                    res = arki::Metadata::read_file(fd, ctx, dest);
                } else if (py_basedir) {
                    arki::metadata::ReadContext ctx(
                            get_fd_name(py_src),
                            std::string(py_basedir, py_basedir_len));
                    res = arki::Metadata::read_file(fd, ctx, dest);
                } else if (py_pathname) {
                    arki::metadata::ReadContext ctx(std::string(py_pathname, py_pathname_len));
                    res = arki::Metadata::read_file(fd, ctx, dest);
                } else {
                    arki::metadata::ReadContext ctx(get_fd_name(py_src));
                    res = arki::Metadata::read_file(fd, ctx, dest);
                }
            }

            if (res)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }
};


struct MetadataDef : public Type<MetadataDef, arkipy_Metadata>
{
    constexpr static const char* name = "Metadata";
    constexpr static const char* qual_name = "arkimet.Metadata";
    constexpr static const char* doc = R"(
Arkimet metadata for one data item
)";
    GetSetters<> getsetters;
    Methods<write, make_inline, make_url, to_python, read_bundle> methods;

    static void _dealloc(Impl* self)
    {
        delete self->md;
        self->md = nullptr;
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("Metadata");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("Metadata()");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        // Metadata() should not be invoked as a constructor, and if someone does
        // this is better than a segfault later on
        PyErr_SetString(PyExc_NotImplementedError, "Cursor objects cannot be constructed explicitly");
        return -1;
    }
};

MetadataDef* metadata_def = nullptr;

}

namespace arki {
namespace python {

arkipy_Metadata* metadata_create(std::unique_ptr<Metadata>&& md)
{
    arkipy_Metadata* result = PyObject_New(arkipy_Metadata, arkipy_Metadata_Type);
    if (!result) return nullptr;
    result->md = md.release();
    return result;
}

void register_metadata(PyObject* m)
{
    metadata_def = new MetadataDef;
    metadata_def->define(arkipy_Metadata_Type, m);
}

}
}

