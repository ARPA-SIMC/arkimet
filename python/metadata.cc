#define PY_SSIZE_T_CLEAN
#include "metadata.h"
#include "arki/core/file.h"
#include "arki/formatter.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/stream.h"
#include "arki/stream/text.h"
#include "arki/structured/json.h"
#include "arki/structured/keys.h"
#include "arki/structured/memory.h"
#include "arki/types/note.h"
#include "arki/types/source.h"
#include "common.h"
#include "files.h"
#include "structured.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include <Python.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_Metadata_Type           = nullptr;
PyTypeObject* arkipy_metadata_dest_func_Type = nullptr;
}

namespace {

/*
 * Metadata
 */

struct data : public Getter<data, arkipy_Metadata>
{
    constexpr static const char* name = "data";
    constexpr static const char* doc =
        "get the raw data described by this metadata";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try
        {
            const metadata::Data& data = self->md->get_data();
            std::vector<uint8_t> buf   = data.read();
            return PyBytes_FromStringAndSize((const char*)buf.data(),
                                             buf.size());
        }
        ARKI_CATCH_RETURN_PYO;
    }
};

struct data_size : public Getter<data_size, arkipy_Metadata>
{
    constexpr static const char* name = "data_size";
    constexpr static const char* doc =
        "return the size of the data, if known, else returns 0";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try
        {
            return to_python(self->md->data_size());
        }
        ARKI_CATCH_RETURN_PYO;
    }
};

struct has_source : public MethNoargs<has_source, arkipy_Metadata>
{
    constexpr static const char* name      = "has_source";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "bool";
    constexpr static const char* summary   = "check if a source has been set";

    static PyObject* run(Impl* self)
    {
        try
        {
            if (self->md->has_source())
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct write : public MethKwargs<write, arkipy_Metadata>
{
    constexpr static const char* name = "write";
    constexpr static const char* signature =
        "file: Union[int, BytesIO], format: str='binary', annotate: bool = "
        "False, skip_data: bool = False";
    constexpr static const char* returns = "None";
    constexpr static const char* summary = "Write the metadata to a file";
    constexpr static const char* doc     = R"(
:param file: the output file. The file can be a normal file-like object or an
             integer file or socket handle
:param format: "binary", "yaml", or "json". Default: "binary".
:param annotate: if True, use a :class:`arkimet.Formatter` to add metadata
                 descriptions to the output
:param skip_data: if True, do not write data after the metadata even if the
                  source type is INLINE
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"file", "format", "annotate",
                                       "skip_data", nullptr};
        PyObject* arg_file          = Py_None;
        const char* format          = nullptr;
        int annotate                = 0;
        int skip_data               = 0;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|spp", pass_kwlist(kwlist),
                                         &arg_file, &format, &annotate,
                                         &skip_data))
            return nullptr;

        try
        {
            std::unique_ptr<arki::StreamOutput> out =
                binaryio_stream_output(arg_file);

            if (!format || strcmp(format, "binary") == 0)
            {
                self->md->write(*out, skip_data);
            }
            else if (strcmp(format, "yaml") == 0)
            {
                std::unique_ptr<arki::Formatter> formatter;
                if (annotate)
                    formatter = arki::Formatter::create();
                std::string yaml = self->md->to_yaml(formatter.get());
                out->send_buffer(yaml.data(), yaml.size());
            }
            else if (strcmp(format, "json") == 0)
            {
                std::unique_ptr<arki::Formatter> formatter;
                if (annotate)
                    formatter = arki::Formatter::create();
                std::stringstream buf;
                arki::structured::JSON output(buf);
                self->md->serialise(output, arki::structured::keys_json,
                                    formatter.get());
                out->send_buffer(buf.str().data(), buf.str().size());
            }
            else
            {
                PyErr_Format(PyExc_ValueError,
                             "Unsupported metadata serializati format: %s",
                             format);
                return nullptr;
            }
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct make_absolute : public MethNoargs<make_absolute, arkipy_Metadata>
{
    constexpr static const char* name      = "make_absolute";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "";
    constexpr static const char* summary = "Make path in source blob absolute";

    static PyObject* run(Impl* self)
    {
        try
        {
            self->md->make_absolute();
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct make_inline : public MethNoargs<make_inline, arkipy_Metadata>
{
    constexpr static const char* name      = "make_inline";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "";
    constexpr static const char* summary =
        "Read the data and inline them in the metadata";

    static PyObject* run(Impl* self)
    {
        try
        {
            self->md->makeInline();
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct make_url : public MethKwargs<make_url, arkipy_Metadata>
{
    constexpr static const char* name      = "make_url";
    constexpr static const char* signature = "baseurl: str";
    constexpr static const char* returns   = "None";
    constexpr static const char* summary   = "Set the data source as URL";
    constexpr static const char* doc       = R"(
:param baseurl: the base URL that identifies the dataset
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"baseurl", NULL};
        const char* url             = nullptr;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s", pass_kwlist(kwlist),
                                         &url))
            return nullptr;

        try
        {
            self->md->set_source(
                types::Source::createURL(self->md->source().format, url));
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct to_string : public MethKwargs<to_string, arkipy_Metadata>
{
    constexpr static const char* name      = "to_string";
    constexpr static const char* signature = "type: str=None";
    constexpr static const char* returns   = "Optional[str]";
    constexpr static const char* summary =
        "Return the metadata contents as a string";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"type", NULL};
        const char* py_type         = nullptr;
        Py_ssize_t py_type_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#", pass_kwlist(kwlist),
                                         &py_type, &py_type_len))
            return nullptr;

        try
        {
            pyo_unique_ptr res;
            if (py_type)
            {
                arki::types::Code code = arki::types::parseCodeName(
                    std::string(py_type, py_type_len));
                if (code == arki::TYPE_SOURCE)
                {
                    if (!self->md->has_source())
                        Py_RETURN_NONE;
                    else
                        res = to_python(self->md->source().to_string());
                }
                else
                {
                    const types::Type* item = self->md->get(code);
                    if (!item)
                        Py_RETURN_NONE;
                    res = to_python(item->to_string());
                }
            }
            else
            {
                res.reset(to_python(self->md->to_yaml()));
            }
            return res.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct to_python : public MethKwargs<to_python, arkipy_Metadata>
{
    constexpr static const char* name      = "to_python";
    constexpr static const char* signature = "type: str=None";
    constexpr static const char* returns   = "dict";
    constexpr static const char* summary =
        "Return the metadata contents in a python dict";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"type", NULL};
        const char* py_type         = nullptr;
        Py_ssize_t py_type_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#", pass_kwlist(kwlist),
                                         &py_type, &py_type_len))
            return nullptr;

        try
        {
            arki::python::PythonEmitter e;
            if (py_type)
            {
                arki::types::Code code = arki::types::parseCodeName(
                    std::string(py_type, py_type_len));
                if (code == arki::TYPE_SOURCE)
                {
                    if (!self->md->has_source())
                        Py_RETURN_NONE;
                    else
                        self->md->source().serialise(
                            e, arki::structured::keys_python);
                }
                else
                {
                    const types::Type* item = self->md->get(code);
                    if (!item)
                        Py_RETURN_NONE;
                    item->serialise(e, arki::structured::keys_python);
                }
            }
            else
            {
                self->md->serialise(e, arki::structured::keys_python);
            }
            return e.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct get_notes : public MethNoargs<get_notes, arkipy_Metadata>
{
    constexpr static const char* name      = "get_notes";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "List[Dict[str, Any]]";
    constexpr static const char* summary   = "get the notes for this metadata";

    static PyObject* run(Impl* self)
    {
        try
        {
            auto notes = self->md->notes();
            pyo_unique_ptr res(
                throw_ifnull(PyList_New(notes.second - notes.first)));
            for (auto n = notes.first; n != notes.second; ++n)
            {
                arki::python::PythonEmitter e;
                reinterpret_cast<const arki::types::Note*>(*n)->serialise(
                    e, arki::structured::keys_python);
                // Note This macro “steals” a reference to item, and, unlike
                // PyList_SetItem(), does not discard a reference to any item
                // that is being replaced; any reference in list at position i
                // will be leaked.
                PyList_SET_ITEM(res.get(), n - notes.first, e.release());
            }
            return res.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct del_notes : public MethNoargs<del_notes, arkipy_Metadata>
{
    constexpr static const char* name      = "del_notes";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "";
    constexpr static const char* summary =
        "remove all notes from this Metadata";

    static PyObject* run(Impl* self)
    {
        try
        {
            self->md->clear_notes();
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct read_bundle : public ClassMethKwargs<read_bundle>
{
    constexpr static const char* name = "read_bundle";
    constexpr static const char* signature =
        "src: Union[bytes, ByteIO], dest: Callable[[metadata]=None, "
        "Optional[bool]], basedir: str=None, pathname: str=None";
    constexpr static const char* returns = "Union[bool, List[arkimet.Metadata]";
    constexpr static const char* summary =
        "Read all metadata from a given file or memory buffer";
    constexpr static const char* doc = R"(
:param src: source data or binary input file
:param dest: function called for each metadata decoded. If None, a list of
             arkimet.Metadata is returned instead
:param basedir: base directory used to resolve relative file names inside the metadata
:param pathname: name of the file to use in error messages, when ``src.name`` is not available
)";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"src", "dest", "basedir", "pathname",
                                       nullptr};
        PyObject* py_src            = nullptr;
        PyObject* py_dest           = nullptr;
        const char* py_basedir      = nullptr;
        Py_ssize_t py_basedir_len;
        const char* py_pathname = nullptr;
        Py_ssize_t py_pathname_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|Oz#z#",
                                         const_cast<char**>(kwlist), &py_src,
                                         &py_dest, &py_basedir, &py_basedir_len,
                                         &py_pathname, &py_pathname_len))
            return nullptr;

        try
        {
            pyo_unique_ptr res_list;
            metadata_dest_func dest;
            if (!py_dest or py_dest == Py_None)
            {
                res_list.reset(throw_ifnull(PyList_New(0)));

                dest = [&](std::shared_ptr<Metadata> md) {
                    AcquireGIL gil;
                    pyo_unique_ptr py_md(
                        (PyObject*)throw_ifnull(metadata_create(md)));
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
                    res = arki::Metadata::read_buffer((uint8_t*)buffer, length,
                                                      ctx, dest);
                }
                else if (py_basedir)
                {
                    PyErr_SetString(PyExc_ValueError,
                                    "basedir provided without pathname when "
                                    "parsing metadata from a memory buffer");
                    return nullptr;
                }
                else if (py_pathname)
                {
                    arki::metadata::ReadContext ctx(
                        std::string(py_pathname, py_pathname_len));
                    res = arki::Metadata::read_buffer((uint8_t*)buffer, length,
                                                      ctx, dest);
                }
                else
                {
                    arki::metadata::ReadContext ctx;
                    res = arki::Metadata::read_buffer((uint8_t*)buffer, length,
                                                      ctx, dest);
                }
            }
            else
            {
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
                        res =
                            arki::Metadata::read_file(*in.abstract, ctx, dest);
                }
                else if (py_basedir)
                {
                    if (in.fd)
                    {
                        arki::metadata::ReadContext ctx(
                            in.fd->path(),
                            std::string(py_basedir, py_basedir_len));
                        res = arki::Metadata::read_file(*in.fd, ctx, dest);
                    }
                    else
                    {
                        arki::metadata::ReadContext ctx(
                            in.abstract->path(),
                            std::string(py_basedir, py_basedir_len));
                        res =
                            arki::Metadata::read_file(*in.abstract, ctx, dest);
                    }
                }
                else if (py_pathname)
                {
                    arki::metadata::ReadContext ctx(
                        std::string(py_pathname, py_pathname_len));
                    if (in.fd)
                        res = arki::Metadata::read_file(*in.fd, ctx, dest);
                    else
                        res =
                            arki::Metadata::read_file(*in.abstract, ctx, dest);
                }
                else
                {
                    if (in.fd)
                    {
                        arki::metadata::ReadContext ctx(in.fd->path());
                        res = arki::Metadata::read_file(*in.fd, ctx, dest);
                    }
                    else
                    {
                        arki::metadata::ReadContext ctx(in.abstract->path());
                        res =
                            arki::Metadata::read_file(*in.abstract, ctx, dest);
                    }
                }
            }

            if (res_list)
                return res_list.release();
            if (res)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct write_bundle : public ClassMethKwargs<write_bundle>
{
    constexpr static const char* name = "write_bundle";
    constexpr static const char* signature =
        "mds: Iterable[arkimet.Metadata], file: BinaryIO";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "Write all metadata to a given file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"mds", "file", NULL};
        PyObject* py_mds            = nullptr;
        PyObject* py_file           = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OO", pass_kwlist(kwlist),
                                         &py_mds, &py_file))
            return nullptr;

        try
        {
            std::unique_ptr<arki::StreamOutput> out =
                binaryio_stream_output(py_file);

            metadata::Collection mdc = metadata_collection_from_python(py_mds);
            {
                ReleaseGIL rg;
                mdc.write_to(*out);
            }

            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

// TODO: if one needs to read a stream of yaml metadata, we can implement
// read_yaml_bundle/read_bundle_yaml
struct read_yaml : public ClassMethKwargs<read_yaml>
{
    constexpr static const char* name = "read_yaml";
    constexpr static const char* signature =
        "src: Union[str, StringIO, bytes, ByteIO]";
    constexpr static const char* returns = "Optional[arkimet.Metadata]";
    constexpr static const char* summary = "Read a Metadata from a YAML file";
    constexpr static const char* doc     = "Return None in case of end of file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"src", nullptr};
        PyObject* py_src            = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O",
                                         const_cast<char**>(kwlist), &py_src))
            return nullptr;

        try
        {
            std::shared_ptr<Metadata> res;
            if (PyBytes_Check(py_src))
            {
                char* buffer;
                Py_ssize_t length;
                if (PyBytes_AsStringAndSize(py_src, &buffer, &length) == -1)
                    throw PythonException();
                ReleaseGIL gil;
                auto reader =
                    arki::core::LineReader::from_chars(buffer, length);
                res = Metadata::read_yaml(*reader, "bytes buffer");
            }
            else if (PyUnicode_Check(py_src))
            {
                Py_ssize_t length;
                const char* buffer =
                    throw_ifnull(PyUnicode_AsUTF8AndSize(py_src, &length));
                ReleaseGIL gil;
                auto reader =
                    arki::core::LineReader::from_chars(buffer, length);
                res = Metadata::read_yaml(*reader, "str buffer");
            }
            else if (PyObject_HasAttrString(py_src, "encoding"))
            {
                TextInputFile input(py_src);
                ReleaseGIL gil;

                std::unique_ptr<arki::core::LineReader> reader;
                std::filesystem::path input_name;
                if (input.fd)
                {
                    input_name = input.fd->path();
                    reader     = arki::core::LineReader::from_fd(*input.fd);
                }
                else
                {
                    input_name = input.abstract->path();
                    reader =
                        arki::core::LineReader::from_abstract(*input.abstract);
                }

                res = Metadata::read_yaml(*reader, input_name);
            }
            else
            {
                BinaryInputFile input(py_src);
                ReleaseGIL gil;

                std::unique_ptr<arki::core::LineReader> reader;
                std::string input_name;
                if (input.fd)
                {
                    input_name = input.fd->path();
                    reader     = arki::core::LineReader::from_fd(*input.fd);
                }
                else
                {
                    input_name = input.abstract->path();
                    reader =
                        arki::core::LineReader::from_abstract(*input.abstract);
                }
                res = Metadata::read_yaml(*reader, input_name);
            }

            if (res)
                return (PyObject*)metadata_create(std::move(res));
            else
                Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

// TODO: if one needs to read a stream of json metadata, we can implement
// read_json_bundle/read_bundle_json
struct read_json : public ClassMethKwargs<read_json>
{
    constexpr static const char* name = "read_json";
    constexpr static const char* signature =
        "src: Union[str, StringIO, bytes, ByteIO]";
    constexpr static const char* returns = "arkimet.Metadata";
    constexpr static const char* summary = "Read a Metadata from a JSON file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"src", nullptr};
        PyObject* py_src            = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O",
                                         const_cast<char**>(kwlist), &py_src))
            return nullptr;

        try
        {
            arki::structured::Memory parsed;
            if (PyBytes_Check(py_src))
            {
                char* buffer;
                Py_ssize_t length;
                if (PyBytes_AsStringAndSize(py_src, &buffer, &length) == -1)
                    throw PythonException();
                auto input =
                    arki::core::BufferedReader::from_chars(buffer, length);
                ReleaseGIL gil;
                arki::structured::JSON::parse(*input, parsed);
            }
            else if (PyUnicode_Check(py_src))
            {
                Py_ssize_t length;
                const char* buffer =
                    throw_ifnull(PyUnicode_AsUTF8AndSize(py_src, &length));
                auto input =
                    arki::core::BufferedReader::from_chars(buffer, length);
                ReleaseGIL gil;
                arki::structured::JSON::parse(*input, parsed);
            }
            else if (PyObject_HasAttrString(py_src, "encoding"))
            {
                TextInputFile input(py_src);

                std::unique_ptr<arki::core::BufferedReader> reader;
                if (input.fd)
                    reader = arki::core::BufferedReader::from_fd(*input.fd);
                else
                    reader = arki::core::BufferedReader::from_abstract(
                        *input.abstract);

                ReleaseGIL gil;
                arki::structured::JSON::parse(*reader, parsed);
            }
            else
            {
                BinaryInputFile input(py_src);

                std::unique_ptr<arki::core::BufferedReader> reader;
                if (input.fd)
                    reader = arki::core::BufferedReader::from_fd(*input.fd);
                else
                    reader = arki::core::BufferedReader::from_abstract(
                        *input.abstract);

                ReleaseGIL gil;
                arki::structured::JSON::parse(*reader, parsed);
            }

            ReleaseGIL gil;
            auto res = Metadata::read_structure(arki::structured::keys_json,
                                                parsed.root());

            gil.lock();
            return (PyObject*)metadata_create(std::move(res));
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct print_type_documentation
    : public ClassMethKwargs<print_type_documentation>
{
    constexpr static const char* name      = "print_type_documentation";
    constexpr static const char* signature = "file: Union[TextIO]";
    constexpr static const char* summary =
        "Print documentation about all available metadata types to the given "
        "file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"file", nullptr};
        PyObject* py_file           = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O",
                                         const_cast<char**>(kwlist), &py_file))
            return nullptr;

        try
        {
            std::unique_ptr<arki::StreamOutput> out =
                textio_stream_output(py_file);

            stream::Text textout(*out);
            arki::types::Type::document(textout);

            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct MetadataDef : public Type<MetadataDef, arkipy_Metadata>
{
    constexpr static const char* name      = "Metadata";
    constexpr static const char* qual_name = "arkimet.Metadata";
    constexpr static const char* doc       = R"(
Metadata for one data item.

Each single element stored in Arkimet has a number of metadata associated. This
class stores all the metadata for one single element.

The constructor takes no arguments and returns an empty Metadata object, which
can be populated with item assigment.

Item assigned can take metadata items both in string format or as Python dicts.

For example::

    md = arkimet.Metadata()
    md["origin"] = 'GRIB1(098, 000, 129)'
    md["reftime"] = {"style": "POSITION",  "time": datetime.datetime(2007, 7, 8, 13, 0, 0)}
    with open("test.json", "wb") as fd:
        md.write(fd, format="json")
)";
    GetSetters<data, data_size> getsetters;
    Methods<has_source, write, make_absolute, make_inline, make_url, to_string,
            to_python, get_notes, del_notes, read_bundle, write_bundle,
            read_yaml, read_json, print_type_documentation>
        methods;

    static void _dealloc(Impl* self)
    {
        self->md.~shared_ptr();
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
        static const char* kwlist[] = {nullptr};
        if (!PyArg_ParseTupleAndKeywords(args, kw, "",
                                         const_cast<char**>(kwlist)))
            return -1;

        try
        {
            new (&(self->md))
                std::shared_ptr<arki::Metadata>(make_shared<arki::Metadata>());
        }
        ARKI_CATCH_RETURN_INT

        return 0;
    }

    static PyObject* _richcompare(Impl* self, PyObject* other, int op)
    {
        try
        {
            if (!arkipy_Metadata_Check(other))
                return Py_NotImplemented;
            const arki::Metadata& a = *self->md;
            const arki::Metadata& b = *((Impl*)other)->md;
            switch (op)
            {
                case Py_EQ:
                    if (a == b)
                        Py_RETURN_TRUE;
                    Py_RETURN_FALSE;
                case Py_NE:
                    if (a != b)
                        Py_RETURN_TRUE;
                    Py_RETURN_FALSE;
                case Py_LT: Py_RETURN_NOTIMPLEMENTED;
                case Py_GT: Py_RETURN_NOTIMPLEMENTED;
                case Py_LE: Py_RETURN_NOTIMPLEMENTED;
                case Py_GE: Py_RETURN_NOTIMPLEMENTED;
                default:    Py_RETURN_NOTIMPLEMENTED;
            }
        }
        ARKI_CATCH_RETURN_PYO
    }

    static int sq_contains(Impl* self, PyObject* py_key)
    {
        try
        {
            std::string key  = from_python<std::string>(py_key);
            types::Code code = types::parseCodeName(key);
            if (code == arki::TYPE_SOURCE)
                return self->md->has_source() ? 1 : 0;
            else
                return self->md->has(code) ? 1 : 0;
        }
        ARKI_CATCH_RETURN_INT
    }

    static PyObject* mp_subscript(Impl* self, PyObject* py_key)
    {
        try
        {
            std::string key  = from_python<std::string>(py_key);
            types::Code code = types::parseCodeName(key);
            if (code == TYPE_SOURCE)
            {
                if (!self->md->has_source())
                    return PyErr_Format(PyExc_KeyError,
                                        "section not found: '%s'", key.c_str());
                return python::to_python(self->md->source().to_string());
            }
            else
            {
                const types::Type* res = self->md->get(code);
                if (!res)
                    return PyErr_Format(PyExc_KeyError,
                                        "section not found: '%s'", key.c_str());
                return python::to_python(res->to_string());
            }
        }
        ARKI_CATCH_RETURN_PYO
    }

    static int mp_ass_subscript(Impl* self, PyObject* py_key, PyObject* py_val)
    {
        try
        {
            std::string key  = from_python<std::string>(py_key);
            types::Code code = types::parseCodeName(key);
            if (!py_val)
            {
                if (code == TYPE_SOURCE)
                    self->md->unset_source();
                else
                    self->md->unset(code);
            }
            else
            {
                if (PyUnicode_Check(py_val))
                {
                    std::string strval = from_python<std::string>(py_val);
                    std::unique_ptr<types::Type> val =
                        types::decodeString(code, strval);
                    if (code == TYPE_SOURCE)
                    {
                        std::unique_ptr<types::Source> s(
                            reinterpret_cast<types::Source*>(val.release()));
                        self->md->set_source(std::move(s));
                    }
                    else
                        self->md->set(std::move(val));
                }
                else
                {
                    PythonReader reader(py_val);
                    std::unique_ptr<types::Type> val = types::decode_structure(
                        arki::structured::keys_python, code, reader);
                    if (code == TYPE_SOURCE)
                    {
                        std::unique_ptr<types::Source> s(
                            reinterpret_cast<types::Source*>(val.release()));
                        self->md->set_source(std::move(s));
                    }
                    else
                        self->md->set(std::move(val));
                }
            }
            return 0;
        }
        ARKI_CATCH_RETURN_INT
    }
};

MetadataDef* metadata_def = nullptr;

struct PyDestFunc
{
    PyObject* callable;

    PyDestFunc(PyObject* callable) : callable(callable)
    {
        Py_XINCREF(callable);
    }

    PyDestFunc(const PyDestFunc& o) : callable(o.callable)
    {
        Py_XINCREF(callable);
    }

    PyDestFunc(PyDestFunc&& o) : callable(o.callable) { o.callable = nullptr; }

    ~PyDestFunc() { Py_XDECREF(callable); }

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
        callable   = o.callable;
        o.callable = nullptr;
        return *this;
    }

    bool operator()(std::shared_ptr<Metadata> md)
    {
        AcquireGIL gil;
        // call arg_on_metadata
        py_unique_ptr<arkipy_Metadata> pymd(metadata_create(md));
        pyo_unique_ptr args(PyTuple_Pack(1, pymd.get()));
        if (!args)
            throw PythonException();
        pyo_unique_ptr res(PyObject_CallObject(callable, args));
        if (!res)
            throw PythonException();
        // Continue if the callback returns None or True
        if (res == Py_None)
            return true;
        int cont = PyObject_IsTrue(res);
        if (cont == -1)
            throw PythonException();
        return cont == 1;
    }
};

struct MetadataDestFuncDef
    : public Type<MetadataDestFuncDef, arkipy_metadata_dest_func>
{
    constexpr static const char* name      = "metadata_dest_func";
    constexpr static const char* qual_name = "arkimet.metadata_dest_func";
    constexpr static const char* doc       = R"(
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
        static const char* kwlist[] = {"md", nullptr};
        PyObject* py_md             = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!",
                                         const_cast<char**>(kwlist),
                                         arkipy_Metadata_Type, &py_md))
            return nullptr;

        try
        {
            std::shared_ptr<Metadata> md(
                ((arkipy_Metadata*)py_md)->md->clone());
            bool res = self->func(std::move(md));
            if (res)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        }
        ARKI_CATCH_RETURN_PYO
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        PyErr_SetString(
            PyExc_NotImplementedError,
            "MetadataDestFunc objects cannot be constructed explicitly");
        return -1;
    }
};

MetadataDestFuncDef* metadata_dest_func_def = nullptr;

} // namespace

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
        }
        else
        {
            PyErr_SetString(PyExc_TypeError,
                            "an iterable of arkimet.Metadata is needed");
            throw PythonException();
        }
    }

    if (PyErr_Occurred())
        throw PythonException();

    return mdc;
}

arkipy_Metadata* metadata_create(std::unique_ptr<Metadata> md)
{
    arkipy_Metadata* result =
        PyObject_New(arkipy_Metadata, arkipy_Metadata_Type);
    if (!result)
        throw PythonException();
    new (&(result->md)) std::shared_ptr<Metadata>(std::move(md));
    return result;
}

arkipy_Metadata* metadata_create(std::shared_ptr<Metadata> md)
{
    arkipy_Metadata* result =
        PyObject_New(arkipy_Metadata, arkipy_Metadata_Type);
    if (!result)
        throw PythonException();
    new (&(result->md)) std::shared_ptr<Metadata>(md);
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
    arkipy_metadata_dest_func* result =
        PyObject_New(arkipy_metadata_dest_func, arkipy_metadata_dest_func_Type);
    if (!result)
        throw PythonException();
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

} // namespace python
} // namespace arki
