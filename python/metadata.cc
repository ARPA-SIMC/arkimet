#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "metadata.h"
#include "common.h"
#include "structured.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/core/file.h"
#include "arki/types/source.h"
#include "arki/structured/keys.h"
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
PyTypeObject* arkipy_metadata_dest_func_Type = nullptr;

}

namespace {

/*
 * Metadata
 */

struct data : public Getter<data, arkipy_Metadata>
{
    constexpr static const char* name = "data";
    constexpr static const char* doc = "get the raw data described by this metadata";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try {
            const metadata::Data& data = self->md->get_data();
            std::vector<uint8_t> buf = data.read();
            return PyBytes_FromStringAndSize((const char*)buf.data(), buf.size());
        } ARKI_CATCH_RETURN_PYO;
    }
};

struct data_size : public Getter<data_size, arkipy_Metadata>
{
    constexpr static const char* name = "data_size";
    constexpr static const char* doc = "return the size of the data, if known, else returns 0";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try {
            return to_python(self->md->data_size());
        } ARKI_CATCH_RETURN_PYO;
    }
};

struct has_source : public MethNoargs<has_source, arkipy_Metadata>
{
    constexpr static const char* name = "has_source";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "bool";
    constexpr static const char* summary = "check if a source has been set";

    static PyObject* run(Impl* self)
    {
        try {
            if (self->md->has_source())
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct write : public MethKwargs<write, arkipy_Metadata>
{
    constexpr static const char* name = "write";
    constexpr static const char* signature = "file: Union[int, BytesIO], format: str";
    constexpr static const char* returns = "None";
    constexpr static const char* summary = "Write the metadata to a file";
    constexpr static const char* doc = R"(
Arguments:
  file: the output file. The file can be a normal file-like object or an
        integer file or socket handle
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

struct to_string : public MethKwargs<to_string, arkipy_Metadata>
{
    constexpr static const char* name = "to_string";
    constexpr static const char* signature = "type: str=None";
    constexpr static const char* returns = "Optional[str]";
    constexpr static const char* summary = "Return the metadata contents as a string";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "type", NULL };
        const char* py_type = nullptr;
        Py_ssize_t py_type_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#", (char**)kwlist, &py_type, &py_type_len))
            return nullptr;

        try {
            pyo_unique_ptr res;
            if (py_type)
            {
                arki::types::Code code = arki::types::parseCodeName(std::string(py_type, py_type_len));
                if (code == arki::TYPE_SOURCE)
                {
                    if (!self->md->has_source())
                        Py_RETURN_NONE;
                    else
                        res = to_python(self->md->source().to_string());
                } else {
                    const types::Type* item = self->md->get(code);
                    if (!item)
                        Py_RETURN_NONE;
                    res = to_python(item->to_string());
                }
            } else {
                res.reset(to_python(self->md->to_yaml()));
            }
            return res.release();
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
        static const char* kwlist[] = { "type", NULL };
        const char* py_type = nullptr;
        Py_ssize_t py_type_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#", (char**)kwlist, &py_type, &py_type_len))
            return nullptr;

        try {
            arki::python::PythonEmitter e;
            if (py_type)
            {
                arki::types::Code code = arki::types::parseCodeName(std::string(py_type, py_type_len));
                if (code == arki::TYPE_SOURCE)
                {
                    if (!self->md->has_source())
                        Py_RETURN_NONE;
                    else
                        self->md->source().serialise(e, arki::structured::keys_python);
                } else {
                    const types::Type* item = self->md->get(code);
                    if (!item)
                        Py_RETURN_NONE;
                    item->serialise(e, arki::structured::keys_python);
                }
            } else {
                self->md->serialise(e, arki::structured::keys_python);
            }
            return e.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct get_notes : public MethNoargs<get_notes, arkipy_Metadata>
{
    constexpr static const char* name = "get_notes";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "List[Dict[str, Any]]";
    constexpr static const char* summary = "get the notes for this metadata";

    static PyObject* run(Impl* self)
    {
        try {
            std::vector<types::Note> notes = self->md->notes();
            pyo_unique_ptr res(throw_ifnull(PyList_New(notes.size())));
            for (unsigned idx = 0; idx < notes.size(); ++idx)
            {
                arki::python::PythonEmitter e;
                notes[idx].serialise(e, arki::structured::keys_python);
                // Note This macro “steals” a reference to item, and, unlike
                // PyList_SetItem(), does not discard a reference to any item
                // that is being replaced; any reference in list at position i
                // will be leaked.
                PyList_SET_ITEM(res.get(), idx, e.release());
            }
            return res.release();
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
                    AcquireGIL gil;
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
                ReleaseGIL gil;

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
    GetSetters<data, data_size> getsetters;
    Methods<has_source, write, make_absolute, make_inline, make_url, to_string, to_python, get_notes, read_bundle, write_bundle> methods;

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

    static int sq_contains(Impl* self, PyObject* py_key)
    {
        try {
            std::string key = from_python<std::string>(py_key);
            types::Code code = types::parseCodeName(key);
            if (code == arki::TYPE_SOURCE)
                return self->md->has_source() ? 1 : 0;
            else
                return self->md->has(code) ? 1 : 0;
        } ARKI_CATCH_RETURN_INT
    }

    static PyObject* mp_subscript(Impl* self, PyObject* py_key)
    {
        try {
            std::string key = from_python<std::string>(py_key);
            types::Code code = types::parseCodeName(key);
            if (code == TYPE_SOURCE)
            {
                if (!self->md->has_source())
                    return PyErr_Format(PyExc_KeyError, "section not found: '%s'", key.c_str());
                return python::to_python(self->md->source().to_string());
            } else {
                const types::Type* res = self->md->get(code);
                if (!res)
                    return PyErr_Format(PyExc_KeyError, "section not found: '%s'", key.c_str());
                return python::to_python(res->to_string());
            }
        } ARKI_CATCH_RETURN_PYO
    }

    static int mp_ass_subscript(Impl* self, PyObject* py_key, PyObject* py_val)
    {
        try {
            std::string key = from_python<std::string>(py_key);
            types::Code code = types::parseCodeName(key);
            if (!py_val)
            {
                self->md->unset(code);
            } else {
                if (PyUnicode_Check(py_val))
                {
                    std::string strval = from_python<std::string>(py_val);
                    std::unique_ptr<types::Type> val = types::decodeString(code, strval);
                    self->md->set(std::move(val));
                } else {
                    PythonReader reader(py_val);
                    std::unique_ptr<types::Type> val = types::decode_structure(arki::structured::keys_python, reader);
                    self->md->set(std::move(val));
                }
            }
            return 0;
        } ARKI_CATCH_RETURN_INT
    }
};

MetadataDef* metadata_def = nullptr;


struct PyDestFunc
{
    PyObject* callable;

    PyDestFunc(PyObject* callable)
        : callable(callable)
    {
        Py_XINCREF(callable);
    }

    PyDestFunc(const PyDestFunc& o)
        : callable(o.callable)
    {
        Py_XINCREF(callable);
    }

    PyDestFunc(PyDestFunc&& o)
        : callable(o.callable)
    {
        o.callable = nullptr;
    }

    ~PyDestFunc()
    {
        Py_XDECREF(callable);
    }

    PyDestFunc& operator=(const PyDestFunc& o)
    {
        Py_XINCREF(o.callable);
        Py_XDECREF(callable);
        callable = o.callable;
        return *this;
    }

    PyDestFunc& operator=(PyDestFunc&& o)
    {
        if (this == &o)
            return *this;

        Py_XDECREF(callable);
        callable = o.callable;
        o.callable = nullptr;
        return *this;
    }

    bool operator()(std::unique_ptr<Metadata> md)
    {
        AcquireGIL gil;
        // call arg_on_metadata
        py_unique_ptr<arkipy_Metadata> pymd(metadata_create(std::move(md)));
        pyo_unique_ptr args(PyTuple_Pack(1, pymd.get()));
        if (!args) throw PythonException();
        pyo_unique_ptr res(PyObject_CallObject(callable, args));
        if (!res) throw PythonException();
        // Continue if the callback returns None or True
        if (res == Py_None) return true;
        int cont = PyObject_IsTrue(res);
        if (cont == -1) throw PythonException();
        return cont == 1;
    }
};


struct MetadataDestFuncDef : public Type<MetadataDestFuncDef, arkipy_metadata_dest_func>
{
    constexpr static const char* name = "metadata_dest_func";
    constexpr static const char* qual_name = "arkimet.metadata_dest_func";
    constexpr static const char* doc = R"(
callback for producing one metadata element
)";
    GetSetters<> getsetters;
    Methods<> methods;

    static void _dealloc(Impl* self)
    {
        self->func.~metadata_dest_func();
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString("metadata_dest_func");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromString("metadata_dest_func");
    }

    static PyObject* _call(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "md", nullptr };
        PyObject* py_md = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!",
                    const_cast<char**>(kwlist), arkipy_Metadata_Type, &py_md))
            return nullptr;

        try {
            std::unique_ptr<Metadata> md(new Metadata(*((arkipy_Metadata*)py_md)->md));
            bool res = self->func(std::move(md));
            if (res)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        PyErr_SetString(PyExc_NotImplementedError, "MetadataDestFunc objects cannot be constructed explicitly");
        return -1;
    }
};

MetadataDestFuncDef* metadata_dest_func_def = nullptr;


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


arki::metadata_dest_func dest_func_from_python(PyObject* o)
{
    if (PyCallable_Check(o))
    {
        return PyDestFunc(o);
    }
    PyErr_SetString(PyExc_TypeError, "value must be a callable");
    throw PythonException();
}

PyObject* dest_func_to_python(arki::metadata_dest_func func)
{
    arkipy_metadata_dest_func* result = PyObject_New(arkipy_metadata_dest_func, arkipy_metadata_dest_func_Type);
    if (!result) return nullptr;
    new (&(result->func)) metadata_dest_func(func);
    return (PyObject*)result;
}


void register_metadata(PyObject* m)
{
    metadata_def = new MetadataDef;
    metadata_def->define(arkipy_Metadata_Type, m);

    metadata_dest_func_def = new MetadataDestFuncDef;
    metadata_dest_func_def->define(arkipy_metadata_dest_func_Type);
}

}
}

