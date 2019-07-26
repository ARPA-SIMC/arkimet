#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "metadata.h"
#include "common.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/core/file.h"
#include "arki/types/source.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "files.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
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
            BinaryOutputFile out(arg_file);

            if (!format || strcmp(format, "binary") == 0)
            {
                if (out.fd)
                    self->md->write(*out.fd);
                else
                    self->md->write(*out.abstract);
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

struct make_absolute : public MethNoargs<make_absolute, arkipy_Metadata>
{
    constexpr static const char* name = "make_absolute";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "Make path in source blob absolute";

    static PyObject* run(Impl* self)
    {
        try {
            self->md->make_absolute();
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

struct read_bundle : public ClassMethKwargs<read_bundle>
{
    constexpr static const char* name = "read_bundle";
    constexpr static const char* signature = "src: Union[bytes, ByteIO], dest: Callable[[metadata]=None, Optional[bool]], basedir: str=None, pathname: str=None";
    constexpr static const char* returns = "Union[bool, List[arki.Metadata]";
    constexpr static const char* summary = "Read all metadata from a given file or memory buffer";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "src", "dest", "basedir", "pathname", nullptr };
        PyObject* py_src = nullptr;
        PyObject* py_dest = nullptr;
        const char* py_basedir = nullptr;
        Py_ssize_t py_basedir_len;
        const char* py_pathname = nullptr;
        Py_ssize_t py_pathname_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|Oz#z#",
                    const_cast<char**>(kwlist), &py_src, &py_dest,
                    &py_basedir, &py_basedir_len, &py_pathname, &py_pathname_len))
            return nullptr;

        try {
            pyo_unique_ptr res_list;
            metadata_dest_func dest;
            if (!py_dest or py_dest == Py_None)
            {
                res_list.reset(throw_ifnull(PyList_New(0)));

                dest = [&](std::unique_ptr<Metadata> md) {
                    pyo_unique_ptr py_md((PyObject*)throw_ifnull(metadata_create(std::move(md))));
                    if (PyList_Append(res_list, py_md) == -1)
                        throw PythonException();
                    return true;
                };
            }
            else
                dest = dest_func_from_python(py_dest);

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
                BinaryInputFile in(py_src);

                if (py_basedir && py_pathname)
                {
                    arki::metadata::ReadContext ctx(
                            std::string(py_pathname, py_pathname_len),
                            std::string(py_basedir, py_basedir_len));
                    if (in.fd)
                        res = arki::Metadata::read_file(*in.fd, ctx, dest);
                    else
                        res = arki::Metadata::read_file(*in.abstract, ctx, dest);
                } else if (py_basedir) {
                    if (in.fd)
                    {
                        arki::metadata::ReadContext ctx(
                                in.fd->name(), std::string(py_basedir, py_basedir_len));
                        res = arki::Metadata::read_file(*in.fd, ctx, dest);
                    }
                    else
                    {
                        arki::metadata::ReadContext ctx(
                                in.abstract->name(), std::string(py_basedir, py_basedir_len));
                        res = arki::Metadata::read_file(*in.abstract, ctx, dest);
                    }
                } else if (py_pathname) {
                    arki::metadata::ReadContext ctx(std::string(py_pathname, py_pathname_len));
                    if (in.fd)
                        res = arki::Metadata::read_file(*in.fd, ctx, dest);
                    else
                        res = arki::Metadata::read_file(*in.abstract, ctx, dest);
                } else {
                    if (in.fd)
                    {
                        arki::metadata::ReadContext ctx(in.fd->name());
                        res = arki::Metadata::read_file(*in.fd, ctx, dest);
                    }
                    else
                    {
                        arki::metadata::ReadContext ctx(in.abstract->name());
                        res = arki::Metadata::read_file(*in.abstract, ctx, dest);
                    }
                }
            }

            if (res_list)
                return res_list.release();
            if (res)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }
};


struct write_bundle : public ClassMethKwargs<write_bundle>
{
    constexpr static const char* name = "write_bundle";
    constexpr static const char* signature = "mds: Iterable[arkimet.Metadata], file: BinaryIO";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "Write all metadata to a given file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "mds", "file", NULL };
        PyObject* py_mds = nullptr;
        PyObject* py_file = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OO", (char**)kwlist, &py_mds, &py_file))
            return nullptr;

        try {
            BinaryOutputFile out(py_file);

            metadata::Collection mdc = metadata_collection_from_python(py_mds);
            {
                ReleaseGIL rg;
                if (out.fd)
                    mdc.write_to(*out.fd);
                else
                    mdc.write_to(*out.abstract);
            }

            Py_RETURN_NONE;
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
    Methods<write, make_absolute, make_inline, make_url, to_python, read_bundle, write_bundle> methods;

    static void _dealloc(Impl* self)
    {
        delete self->md;
        self->md = nullptr;
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        std::string yaml = self->md->to_yaml();
        return PyUnicode_FromStringAndSize(yaml.data(), yaml.size());
    }

    static PyObject* _repr(Impl* self)
    {
        std::string yaml = self->md->to_yaml();
        return PyUnicode_FromStringAndSize(yaml.data(), yaml.size());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        // Metadata() should not be invoked as a constructor, and if someone does
        // this is better than a segfault later on
        PyErr_SetString(PyExc_NotImplementedError, "Cursor objects cannot be constructed explicitly");
        return -1;
    }

    static PyObject* _richcompare(Impl* self, PyObject *other, int op)
    {
        try {
            if (!arkipy_Metadata_Check(other))
                return Py_NotImplemented;
            Py_RETURN_RICHCOMPARE(*(self->md), *(((Impl*)other)->md), op);
        } ARKI_CATCH_RETURN_PYO
    }
};

MetadataDef* metadata_def = nullptr;

}

namespace arki {
namespace python {

arki::metadata::Collection metadata_collection_from_python(PyObject* o)
{
    metadata::Collection mdc;
    pyo_unique_ptr iterator(throw_ifnull(PyObject_GetIter(o)));

    while (true)
    {
        pyo_unique_ptr item(PyIter_Next(iterator));
        if (!item)
            break;

        if (arkipy_Metadata_Check(item))
        {
            mdc.push_back(*((arkipy_Metadata*)item.get())->md);
        } else {
            PyErr_SetString(PyExc_TypeError, "an iterable of arkimet.Metadata is needed");
            throw PythonException();
        }
    }

    if (PyErr_Occurred())
        throw PythonException();

    return mdc;
}

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

