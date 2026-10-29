#include "grib.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/nag.h"
#include "arki/python/common.h"
#include "arki/python/files.h"
#include "arki/python/metadata.h"
#include "arki/python/scan.h"
#include "arki/python/utils/methods.h"
#include "arki/python/utils/type.h"
#include "arki/types/source.h"
#include <grib_api.h>

extern "C" {
PyTypeObject* arkipy_scan_Grib_Type       = nullptr;
PyTypeObject* arkipy_scan_GribReader_Type = nullptr;
}

namespace arki::python::scan {

inline void check_grib_error(int res, const char* msg)
{
    if (res)
    {
        PyErr_Format(PyExc_KeyError, "%s: %s", msg,
                     grib_get_error_message(res));
        throw PythonException();
    }
}

inline void check_grib_lookup_error(int res, const char* key, const char* msg)
{
    if (res)
    {
        PyErr_Format(PyExc_KeyError, "%s, key: %s: %s", msg, key,
                     grib_get_error_message(res));
        throw PythonException();
    }
}

static arkipy_scan_Grib* grib_create(grib_handle* gh, bool owns_handle = false)
{
    arkipy_scan_Grib* result =
        PyObject_New(arkipy_scan_Grib, arkipy_scan_Grib_Type);
    if (!result)
        throw PythonException();
    result->gh          = gh;
    result->owns_handle = owns_handle;
    return result;
}

PyObject* gribscanner_object = nullptr;

static void load_gribscanner_object()
{
    load_scanner_scripts();

    // Get arkimet.scan.grib.GribScanner
    pyo_unique_ptr module(
        throw_ifnull(PyImport_ImportModule("arkimet.scan.grib")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    gribscanner_object = obj.release();
}

/*
 * Grib python class
 */

struct edition : public Getter<edition, arkipy_scan_Grib>
{
    constexpr static const char* name = "edition";
    constexpr static const char* doc  = "return the GRIB edition";
    constexpr static void* closure    = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try
        {
            long edition;
            check_grib_error(grib_get_long(self->gh, "editionNumber", &edition),
                             "cannot read edition number");
            return to_python((int)edition);
        }
        ARKI_CATCH_RETURN_PYO;
    }
};

struct get_long : public MethKwargs<get_long, arkipy_scan_Grib>
{
    constexpr static const char* name      = "get_long";
    constexpr static const char* signature = "str, int | None";
    constexpr static const char* returns   = "int";
    constexpr static const char* summary =
        "return the long value of a grib key";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"key", "default", NULL};
        const char* key             = nullptr;
        PyObject* arg_default       = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s|O", pass_kwlist(kwlist),
                                         &key, &arg_default))
            return nullptr;

        try
        {
            long val;
            int res = grib_get_long(self->gh, key, &val);
            if (res == GRIB_NOT_FOUND or val == GRIB_MISSING_LONG)
            {
                if (arg_default)
                {
                    Py_INCREF(arg_default);
                    return arg_default;
                }
                else
                    Py_RETURN_NONE;
            }

            check_grib_lookup_error(res, key, "cannot read long value");

            return to_python(val);
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct get_string : public MethKwargs<get_string, arkipy_scan_Grib>
{
    constexpr static const char* name      = "get_string";
    constexpr static const char* signature = "str, int | None";
    constexpr static const char* returns   = "int";
    constexpr static const char* summary =
        "return the long value of a grib key";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"key", "default", NULL};
        const char* key             = nullptr;
        PyObject* arg_default       = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s|O", pass_kwlist(kwlist),
                                         &key, &arg_default))
            return nullptr;

        try
        {
            const int maxsize = 1000;
            char buf[maxsize];
            size_t len = maxsize;
            int res    = grib_get_string(self->gh, key, buf, &len);
            if (res == GRIB_NOT_FOUND)
            {
                if (arg_default)
                {
                    Py_INCREF(arg_default);
                    return arg_default;
                }
                else
                    Py_RETURN_NONE;
            }
            check_grib_lookup_error(res, key, "cannot read string value");

            buf[len] = 0;
            return to_python(buf);
        }
        ARKI_CATCH_RETURN_PYO
    }
};

typedef MethGenericEnter<arkipy_scan_Grib> grib__enter__;

struct grib__exit__ : MethVarargs<grib__exit__, arkipy_scan_Grib>
{
    constexpr static const char* name = "__exit__";
    constexpr static const char* doc  = "Context manager __exit__";
    static PyObject* run(Impl* self, PyObject* args)
    {
        PyObject* exc_type;
        PyObject* exc_val;
        PyObject* exc_tb;
        if (!PyArg_ParseTuple(args, "OOO", &exc_type, &exc_val, &exc_tb))
            return nullptr;

        try
        {
            if (self->owns_handle and self->gh)
            {
                grib_handle_delete(self->gh);
                self->gh = nullptr;
            }
        }
        ARKI_CATCH_RETURN_PYO

        Py_RETURN_NONE;
    }
};

struct GribDef : public Type<GribDef, arkipy_scan_Grib>
{
    constexpr static const char* name      = "Grib";
    constexpr static const char* qual_name = "arkimet.scan.grib.Grib";
    constexpr static const char* doc       = R"(
Access grib message contents
)";
    GetSetters<edition> getsetters;
    Methods<grib__enter__, grib__exit__, get_long, get_string> methods;

    static void _dealloc(Impl* self)
    {
        if (self->owns_handle and self->gh)
            grib_handle_delete(self->gh);
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self) { return PyUnicode_FromString("Grib"); }

    static PyObject* _repr(Impl* self) { return PyUnicode_FromString("Grib"); }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        // Grib() should not be invoked as a constructor
        PyErr_SetString(PyExc_NotImplementedError,
                        "Grib objects cannot be constructed explicitly");
        return -1;
    }

    static int sq_contains(Impl* self, PyObject* py_key)
    {
        try
        {
            std::string key = from_python<std::string>(py_key);

            // Get information about the type
            int type;
            int res = grib_get_native_type(self->gh, key.c_str(), &type);
            if (res == GRIB_NOT_FOUND)
                return 0;
            else
                check_grib_lookup_error(res, key.c_str(),
                                        "cannot get type of key");

            return 1;
        }
        ARKI_CATCH_RETURN_INT
    }

    static PyObject* mp_subscript(Impl* self, PyObject* py_key)
    {
        try
        {
            std::string key = from_python<std::string>(py_key);

            // Get information about the type
            int type;
            int res = grib_get_native_type(self->gh, key.c_str(), &type);
            if (res == GRIB_NOT_FOUND)
                type = GRIB_TYPE_MISSING;
            else
                check_grib_lookup_error(res, key.c_str(),
                                        "cannot get type of key");

            // Look up the value
            switch (type)
            {
                case GRIB_TYPE_LONG: {
                    long val;
                    check_grib_lookup_error(
                        grib_get_long(self->gh, key.c_str(), &val), key.c_str(),
                        "cannot read reading long value");
                    if (val == GRIB_MISSING_LONG)
                        Py_RETURN_NONE;
                    return to_python(val);
                }
                case GRIB_TYPE_DOUBLE: {
                    double val;
                    check_grib_lookup_error(
                        grib_get_double(self->gh, key.c_str(), &val),
                        key.c_str(), "cannot read double value");
                    if (val == GRIB_MISSING_DOUBLE)
                        Py_RETURN_NONE;
                    return to_python(val);
                }
                case GRIB_TYPE_STRING: {
                    const int maxsize = 1000;
                    char buf[maxsize];
                    size_t len = maxsize;
                    check_grib_lookup_error(
                        grib_get_string(self->gh, key.c_str(), buf, &len),
                        key.c_str(), "cannot read string value");
                    buf[len] = 0;
                    return to_python(buf);
                }
                case GRIB_TYPE_BYTES: {
                    const int maxsize = 1000;
                    unsigned char buf[maxsize];
                    size_t len = maxsize;
                    check_grib_lookup_error(
                        grib_get_bytes(self->gh, key.c_str(), buf, &len),
                        key.c_str(), "cannot read string value");
                    return throw_ifnull(
                        PyBytes_FromStringAndSize((const char*)buf, len));
                }
                default: Py_RETURN_NONE;
            }
        }
        ARKI_CATCH_RETURN_PYO
    }
};

GribDef* grib_def = nullptr;

/*
 * GribReader python class
 */

typedef MethGenericEnter<arkipy_scan_GribReader> gribreader__enter__;

struct gribreader__exit__
    : MethVarargs<gribreader__exit__, arkipy_scan_GribReader>
{
    constexpr static const char* name = "__exit__";
    constexpr static const char* doc  = "Context manager __exit__";
    static PyObject* run(Impl* self, PyObject* args)
    {
        PyObject* exc_type;
        PyObject* exc_val;
        PyObject* exc_tb;
        if (!PyArg_ParseTuple(args, "OOO", &exc_type, &exc_val, &exc_tb))
            return nullptr;

        try
        {
            if (self->scan)
            {
                delete self->scan;
                self->scan = nullptr;
            }
        }
        ARKI_CATCH_RETURN_PYO

        Py_RETURN_NONE;
    }
};

struct gribreader_from_path : public ClassMethKwargs<gribreader_from_path>
{
    constexpr static const char* name      = "from_path";
    constexpr static const char* signature = "path: str";
    constexpr static const char* returns   = "arkimet.scan.grib.GribReader";
    constexpr static const char* summary =
        "Return a GribReader iterating the given file";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"path", nullptr};
        PyObject* arg_path          = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", pass_kwlist(kwlist),
                                         &arg_path))
            return nullptr;

        try
        {
            auto path = path_from_python(arg_path);
            py_unique_ptr<arkipy_scan_GribReader> result(
                throw_ifnull(PyObject_New(arkipy_scan_GribReader,
                                          arkipy_scan_GribReader_Type)));
            result->scan = new arki::data::grib::ScanFile(path);
            return (PyObject*)result.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct gribreader_from_pipe : public ClassMethKwargs<gribreader_from_pipe>
{
    constexpr static const char* name      = "from_pipe";
    constexpr static const char* signature = "file: IO[bytes]";
    constexpr static const char* returns   = "arkimet.scan.grib.GribReader";
    constexpr static const char* summary =
        "Return a GribReader iterating the given file.";
    constexpr static const char* doc =
        "The file must have a working fileno() method.";

    static std::unique_ptr<arki::data::grib::ScanPipe>
    from_arkifile(arkipy_File* file)
    {
        return std::make_unique<arki::data::grib::ScanPipe>(file->fd);
    }

    static std::unique_ptr<arki::data::grib::ScanPipe>
    from_pyfile(PyObject* file)
    {
        /* From python 3.9
        pyo_unique_ptr py_fileno(
            throw_ifnull(PyObject_CallMethodNoArgs(file, "fileno")));
            */
        pyo_unique_ptr py_fileno(
            throw_ifnull(PyObject_CallMethod(file, "fileno", NULL)));
        pyo_unique_ptr py_name(
            throw_ifnull(PyObject_GetAttrString(file, "name")));

        int fileno                 = int_from_python(py_fileno);
        std::filesystem::path path = path_from_python(py_name);
        core::NamedFileDescriptor fd(fileno, path);
        return std::make_unique<arki::data::grib::ScanPipe>(fd);
    }

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"file", nullptr};
        PyObject* arg_file          = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", pass_kwlist(kwlist),
                                         &arg_file))
            return nullptr;

        try
        {
            std::unique_ptr<arki::data::grib::ScanPipe> scanner;
            if (arkipy_File_Check(arg_file))
            {
                scanner = from_arkifile((arkipy_File*)arg_file);
            }
            else
            {
                scanner = from_pyfile(arg_file);
            }
            py_unique_ptr<arkipy_scan_GribReader> result(
                throw_ifnull(PyObject_New(arkipy_scan_GribReader,
                                          arkipy_scan_GribReader_Type)));
            result->scan = scanner.release();
            return (PyObject*)result.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct gribreader_from_bytes : public ClassMethKwargs<gribreader_from_bytes>
{
    constexpr static const char* name      = "from_bytes";
    constexpr static const char* signature = "data: bytes";
    constexpr static const char* returns   = "arkimet.scan.grib.GribReader";
    constexpr static const char* summary =
        "Return a GribReader reading one GRIB from the given bytes";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"data", nullptr};
        PyObject* arg_data          = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", pass_kwlist(kwlist),
                                         &arg_data))
            return nullptr;

        try
        {
            char* pydata;
            Py_ssize_t pydata_len;
            if (PyBytes_AsStringAndSize(arg_data, &pydata, &pydata_len) == -1)
                return nullptr;

            const uint8_t* udata = reinterpret_cast<uint8_t*>(pydata);
            std::vector<uint8_t> data(udata, udata + pydata_len);

            py_unique_ptr<arkipy_scan_GribReader> result(
                throw_ifnull(PyObject_New(arkipy_scan_GribReader,
                                          arkipy_scan_GribReader_Type)));
            result->scan = new arki::data::grib::ScanData(data);
            return (PyObject*)result.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct GribReaderDef : public Type<GribReaderDef, arkipy_scan_GribReader>
{
    constexpr static const char* name      = "GribReader";
    constexpr static const char* qual_name = "arkimet.scan.grib.GribReader";
    constexpr static const char* doc       = R"(
Enumerate all GRIBs in a file.

This is used to test the GRIB scanner: iterates arkimet.scan.grib.Grib objects
for each GRIB message in a file.

It implements the context manager protocol so it can be used in a ``with``
statement.

It generates a sequence of ``(arkimet.scan.Grib, arki.Metadata)`` tuples.
)";

    GetSetters<> getsetters;
    Methods<gribreader__enter__, gribreader__exit__, gribreader_from_path,
            gribreader_from_pipe, gribreader_from_bytes>
        methods;

    static void _dealloc(Impl* self)
    {
        if (self->scan)
        {
            delete self->scan;
            self->scan = nullptr;
        }
        Py_TYPE(self)->tp_free(self);
    }

    static int _init(arkipy_scan_GribReader* self, PyObject* args, PyObject* kw)
    {
        PyErr_SetString(
            PyExc_NotImplementedError,
            "use from_* methods to instantiate GribReader instances");
        return -1;
    }

    static PyObject* _iter(arkipy_scan_GribReader* self)
    {
        Py_INCREF(self);
        return (PyObject*)self;
    }

    static PyObject* _iternext(arkipy_scan_GribReader* self)
    {
        try
        {
            if (!self->scan->next())
            {
                PyErr_SetNone(PyExc_StopIteration);
                return nullptr;
            }

            auto md = self->scan->metadata();

            pyo_unique_ptr py_grib(
                (PyObject*)grib_create(self->scan->handle(), false));
            pyo_unique_ptr py_metadata((PyObject*)metadata_create(md));
            return PyTuple_Pack(2, py_grib.get(), py_metadata.get());
        }
        ARKI_CATCH_RETURN_PYO
    }
};

GribReaderDef* grib_reader_def = nullptr;

Methods<> grib_methods;

extern "C" {

static PyModuleDef grib_module = {
    PyModuleDef_HEAD_INIT,
    "grib",                            /* m_name */
    "Arkimet GRIB-specific functions", /* m_doc */
    -1,                                /* m_size */
    grib_methods.as_py(),              /* m_methods */
    nullptr,                           /* m_slots */
    nullptr,                           /* m_traverse */
    nullptr,                           /* m_clear */
    nullptr,                           /* m_free */
};
}

void register_scan_grib(PyObject* scan)
{
    pyo_unique_ptr grib = throw_ifnull(PyModule_Create(&grib_module));

    grib_def = new GribDef;
    grib_def->define(arkipy_scan_Grib_Type, grib);

    grib_reader_def = new GribReaderDef;
    grib_reader_def->define(arkipy_scan_GribReader_Type, grib);

    if (PyModule_AddObject(scan, "grib", grib.release()) == -1)
        throw PythonException();
}
#if 0
void init_scanner_grib()
{
    arki::data::Scanner::register_scanner(
        DataFormat::GRIB, std::make_shared<PythonGribScanner>());
}
#endif
} // namespace arki::python::scan
