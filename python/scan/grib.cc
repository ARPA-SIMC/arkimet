#include "grib.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "common.h"
#include "metadata.h"
#include "scan.h"
#include "utils/methods.h"
#include "utils/type.h"
#include <grib_api.h>

using GribHandle = arki::scan::grib::GribHandle;

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

class PythonGribScanner : public arki::scan::GribScanner
{
protected:
    std::shared_ptr<Metadata> scan(grib_handle* gh) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!gribscanner_object)
            load_gribscanner_object();

        pyo_unique_ptr pygh((PyObject*)grib_create(gh));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
            gribscanner_object, "scan", "OO", pygh.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != 1)
            arki::nag::warning(
                "metadata use count after scanning is %ld instead of 1",
                md.use_count());

        return md;
    }

public:
    PythonGribScanner() {}
    virtual ~PythonGribScanner() {}
};

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

            check_grib_error(res, "cannot read long value from grib");

            return to_python(val);
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
    Methods<grib__enter__, grib__exit__, get_long> methods;

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
            if (self->file)
            {
                fclose(self->file);
                self->file = nullptr;
            }
        }
        ARKI_CATCH_RETURN_PYO

        Py_RETURN_NONE;
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
)";

    GetSetters<> getsetters;
    Methods<gribreader__enter__, gribreader__exit__> methods;

    static void _dealloc(Impl* self)
    {
        if (self->file)
        {
            fclose(self->file);
            self->file = nullptr;
        }
        Py_TYPE(self)->tp_free(self);
    }

    static int _init(arkipy_scan_GribReader* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"path", NULL};
        PyObject* arg_path          = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", pass_kwlist(kwlist),
                                         &arg_path))
            return -1;

        try
        {
            auto path  = path_from_python(arg_path);
            self->file = fopen(path.c_str(), "rb");
            if (!self->file)
                throw_file_error(path, "cannot open file");

            self->context = grib_context_get_default();
            if (!self->context)
                throw std::runtime_error(
                    "cannot get grib_api default context: default "
                    "context is not available");

            // Multi support is off unless a child class specifies otherwise
            grib_multi_support_off(self->context);
        }
        ARKI_CATCH_RETURN_INT
        return 0;
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
            GribHandle gh(self->context, self->file);
            if (!gh)
            {
                PyErr_SetNone(PyExc_StopIteration);
                return nullptr;
            }

            return (PyObject*)grib_create(gh.release(), true);
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
void init_scanner_grib()
{
    arki::scan::Scanner::register_factory(
        DataFormat::GRIB, [] { return std::make_shared<PythonGribScanner>(); });
}
} // namespace arki::python::scan
