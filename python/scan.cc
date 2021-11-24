#include "scan.h"
#include "common.h"
#include "structured.h"
#include "metadata.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "arki/libconfig.h"
#include "arki/metadata.h"
#include "arki/types/values.h"
#include "arki/utils/vm2.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/runtime.h"
#include "arki/scan/grib.h"
#include "arki/scan/bufr.h"
#include "arki/scan/odimh5.h"
#include "arki/scan/netcdf.h"
#include "arki/scan/jpeg.h"
#include "arki/nag.h"
#include <grib_api.h>
#ifdef HAVE_DBALLE
#include "utils/wreport.h"
#include "utils/dballe.h"
#include <dballe/message.h>
#include <dballe/var.h>
#include <wreport/python.h>
#endif


using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_scan_Grib_Type = nullptr;
PyTypeObject* arkipy_scan_BufrMessage_Type = nullptr;

}

namespace {

// Pointer to the arkimet module
PyObject* module_arkimet = nullptr;

// Pointer to the scanners module
PyObject* module_scanners = nullptr;

/// Load scripts from the dir_scan configuration directory
void load_scanners()
{
    using namespace arki;

    static bool scanners_loaded = false;

    if (scanners_loaded)
        return;

    // Get the name of the scanners module
    if (!module_scanners || !module_arkimet)
        throw std::runtime_error("load_scanners was called before the _arkimet.scan module has been initialized");

    std::string base = PyModule_GetName(module_arkimet);
    base += ".";
    base += PyModule_GetName(module_scanners);

    std::vector<std::string> sources = arki::Config::get().dir_scan.list_files(".py");
    for (const auto& source: sources)
    {
        std::string basename = str::basename(source);

        // Check if the scanner module had already been imported
        std::string module_name = base + "." + basename.substr(0, basename.size() - 3);
        pyo_unique_ptr py_module_name(string_to_python(module_name));
        pyo_unique_ptr module(ArkiPyImport_GetModule(py_module_name));
        if (PyErr_Occurred())
            throw PythonException();

        // Import it if needed
        if (!module)
        {
            std::string source_code = utils::sys::read_file(source);
            pyo_unique_ptr code(throw_ifnull(Py_CompileStringExFlags(
                            source_code.c_str(), source.c_str(),
                            Py_file_input, nullptr, -1)));
            module = pyo_unique_ptr(throw_ifnull(PyImport_ExecCodeModule(
                            module_name.c_str(), code)));
        }
    }

    scanners_loaded = true;
}


inline void check_grib_error(int res, const char* msg)
{
    if (res)
    {
        PyErr_Format(PyExc_KeyError, "%s: %s", msg, grib_get_error_message(res));
        throw PythonException();
    }
}

inline void check_grib_lookup_error(int res, const char* key, const char* msg)
{
    if (res)
    {
        PyErr_Format(PyExc_KeyError, "%s, key: %s: %s", msg, key, grib_get_error_message(res));
        throw PythonException();
    }
}

arkipy_scan_Grib* grib_create(grib_handle* gh)
{
    arkipy_scan_Grib* result = PyObject_New(arkipy_scan_Grib, arkipy_scan_Grib_Type);
    if (!result) throw PythonException();
    result->gh = gh;
    return result;
}


/*
 * scan.grib module functions
 */

PyObject* gribscanner_object = nullptr;

void load_gribscanner_object()
{
    load_scanners();

    // Get arkimet.scan.grib.GribScanner
    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("arkimet.scan.grib")));
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
            arki::nag::warning("metadata use count after scanning is %ld instead of 1", md.use_count());

        return md;
    }

public:
    PythonGribScanner()
    {
    }
    virtual ~PythonGribScanner()
    {
    }
};


struct edition : public Getter<edition, arkipy_scan_Grib>
{
    constexpr static const char* name = "edition";
    constexpr static const char* doc = "return the GRIB edition";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try {
            long edition;
            check_grib_error(grib_get_long(self->gh, "editionNumber", &edition), "cannot read edition number");
            return to_python((int)edition);
        } ARKI_CATCH_RETURN_PYO;
    }
};

struct get_long : public MethKwargs<get_long, arkipy_scan_Grib>
{
    constexpr static const char* name = "get_long";
    constexpr static const char* signature = "str";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "return the long value of a grib key";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "key", NULL };
        const char* key = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s", (char**)kwlist, &key))
            return nullptr;

        try {
            // Push the function result for lua
            long val;
            int res = grib_get_long(self->gh, key, &val);
            if (res == GRIB_NOT_FOUND)
                Py_RETURN_NONE;

            check_grib_error(res, "cannot read long value from grib");

            return to_python(val);
        } ARKI_CATCH_RETURN_PYO
    }
};

struct GribDef : public Type<GribDef, arkipy_scan_Grib>
{
    constexpr static const char* name = "Grib";
    constexpr static const char* qual_name = "arkimet.scan.grib.Grib";
    constexpr static const char* doc = R"(
Access grib message contents
)";
    GetSetters<edition> getsetters;
    Methods<get_long> methods;

    static void _dealloc(Impl* self)
    {
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString("Grib");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromString("Grib");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        // Grib() should not be invoked as a constructor
        PyErr_SetString(PyExc_NotImplementedError, "Grib objects cannot be constructed explicitly");
        return -1;
    }

    static int sq_contains(Impl* self, PyObject* py_key)
    {
        try {
            std::string key = from_python<std::string>(py_key);

            // Get information about the type
            int type;
            int res = grib_get_native_type(self->gh, key.c_str(), &type);
            if (res == GRIB_NOT_FOUND)
                return 0;
            else
                check_grib_lookup_error(res, key.c_str(), "cannot get type of key");

            return 1;
        } ARKI_CATCH_RETURN_INT
    }

    static PyObject* mp_subscript(Impl* self, PyObject* py_key)
    {
        try {
            std::string key = from_python<std::string>(py_key);

            // Get information about the type
            int type;
            int res = grib_get_native_type(self->gh, key.c_str(), &type);
            if (res == GRIB_NOT_FOUND)
                type = GRIB_TYPE_MISSING;
            else
                check_grib_lookup_error(res, key.c_str(), "cannot get type of key");

            // Look up the value
            switch (type)
            {
                case GRIB_TYPE_LONG: {
                    long val;
                    check_grib_lookup_error(grib_get_long(self->gh, key.c_str(), &val), key.c_str(), "cannot read reading long value");
                    return to_python(val);
                }
                case GRIB_TYPE_DOUBLE: {
                    double val;
                    check_grib_lookup_error(grib_get_double(self->gh, key.c_str(), &val), key.c_str(), "cannot read double value");
                    return to_python(val);
                }
                case GRIB_TYPE_STRING: {
                    const int maxsize = 1000;
                    char buf[maxsize];
                    size_t len = maxsize;
                    check_grib_lookup_error(grib_get_string(self->gh, key.c_str(), buf, &len), key.c_str(), "cannot read string value");
                    buf[len] = 0;
                    return to_python(buf);
                }
                default:
                    Py_RETURN_NONE;
            }
        } ARKI_CATCH_RETURN_PYO
    }

};

GribDef* grib_def = nullptr;


/*
 * scan.bufr module contents
 */

#ifdef HAVE_DBALLE
// Wreport API
arki::python::Wreport wreport_api;

// Dballe API
arki::python::Dballe dballe_api;

PyObject* bufrscanner_object = nullptr;

void load_bufrscanner_object()
{
    load_scanners();

    // Get arkimet.scan.bufr.BufrScanner
    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("arkimet.scan.bufr")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    bufrscanner_object = obj.release();
}


class PythonBufrScanner : public arki::scan::BufrScanner
{
protected:
    void scan_extra(dballe::BinaryMessage& rmsg, std::shared_ptr<dballe::Message> msg, std::shared_ptr<Metadata> md) override
    {
        auto orig_use_count = md.use_count();

        AcquireGIL gil;
        if (!bufrscanner_object)
            load_bufrscanner_object();

        pyo_unique_ptr pymsg(dballe_api.message_create(msg));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
                        bufrscanner_object, "scan", "OO", pymsg.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != orig_use_count)
            arki::nag::warning("metadata use count after scanning is %ld instead of %ld", md.use_count(), orig_use_count);
    }

public:
    PythonBufrScanner()
    {
    }
    virtual ~PythonBufrScanner()
    {
    }
};
#endif

/*
 * scan.odimh5 module contents
 */

PyObject* odimh5scanner_object = nullptr;

void load_odimh5scanner_object()
{
    load_scanners();

    // Get arkimet.scan.odimh5.BufrScanner
    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("arkimet.scan.odimh5")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    odimh5scanner_object = obj.release();
}


class PythonOdimh5Scanner : public arki::scan::OdimScanner
{
protected:
    std::shared_ptr<Metadata> scan_h5_file(const std::string& pathname) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!odimh5scanner_object)
            load_odimh5scanner_object();

        pyo_unique_ptr pyfname(to_python(pathname));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
                        odimh5scanner_object, "scan", "OO", pyfname.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != 1)
            arki::nag::warning("metadata use count after scanning is %ld instead of 1", md.use_count());

        return md;
    }

public:
    PythonOdimh5Scanner()
    {
    }
    virtual ~PythonOdimh5Scanner()
    {
    }
};


/*
 * scan.netcdf module contents
 */

PyObject* ncscanner_object = nullptr;

void load_ncscanner_object()
{
    load_scanners();

    // Get arkimet.scan.nc.BufrScanner
    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("arkimet.scan.nc")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    ncscanner_object = obj.release();
}


class PythonNetCDFScanner : public arki::scan::NetCDFScanner
{
protected:
    std::shared_ptr<Metadata> scan_nc_file(const std::string& pathname) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!ncscanner_object)
            load_ncscanner_object();

        pyo_unique_ptr pyfname(to_python(pathname));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
                        ncscanner_object, "scan", "OO", pyfname.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != 1)
            arki::nag::warning("metadata use count after scanning is %ld instead of 1", md.use_count());

        return md;
    }

public:
    PythonNetCDFScanner()
    {
    }
    virtual ~PythonNetCDFScanner()
    {
    }
};


/*
 * scan.jpeg module contents
 */

PyObject* jpegscanner_object = nullptr;

void load_jpegscanner_object()
{
    load_scanners();

    // Get arkimet.scan.nc.BufrScanner
    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("arkimet.scan.jpeg")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    ncscanner_object = obj.release();
}


class PythonJPEGScanner : public arki::scan::JPEGScanner
{
protected:
    std::shared_ptr<Metadata> scan_jpeg_file(const std::string& pathname) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!ncscanner_object)
            load_jpegscanner_object();

        pyo_unique_ptr pyfname(to_python(pathname));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
                        ncscanner_object, "scan_file", "OO", pyfname.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != 1)
            arki::nag::warning("metadata use count after scanning is %ld instead of 1", md.use_count());

        return md;
    }

    std::shared_ptr<Metadata> scan_jpeg_data(const std::vector<uint8_t>& data) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!ncscanner_object)
            load_jpegscanner_object();

        pyo_unique_ptr pydata(to_python(data));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
                        ncscanner_object, "scan_data", "OO", pydata.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != 1)
            arki::nag::warning("metadata use count after scanning is %ld instead of 1", md.use_count());

        return md;
    }

public:
    PythonJPEGScanner()
    {
    }
    virtual ~PythonJPEGScanner()
    {
    }
};


/*
 * scan.vm2 module functions
 */

struct vm2_get_station : public MethKwargs<vm2_get_station, PyObject>
{
    constexpr static const char* name = "get_station";
    constexpr static const char* signature = "id: int";
    constexpr static const char* returns = "Dict[str, Any]";
    constexpr static const char* summary = "Read the station attributes for a VM2 station ID";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "id", nullptr };
        int id;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "i", const_cast<char**>(kwlist), &id))
            return nullptr;

        try {
            arki::types::ValueBag values(arki::utils::vm2::get_station(id));
            arki::python::PythonEmitter e;
            values.serialise(e);
            return e.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct vm2_get_variable : public MethKwargs<vm2_get_variable, PyObject>
{
    constexpr static const char* name = "get_variable";
    constexpr static const char* signature = "id: int";
    constexpr static const char* returns = "Dict[str, Any]";
    constexpr static const char* summary = "Read the variable attributes for a VM2 variable ID";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "id", nullptr };
        int id;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "i", const_cast<char**>(kwlist), &id))
            return nullptr;

        try {
            arki::types::ValueBag values(arki::utils::vm2::get_variable(id));
            arki::python::PythonEmitter e;
            values.serialise(e);
            return e.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

Methods<> scan_methods;
Methods<> scanners_methods;
Methods<> grib_methods;
Methods<> bufr_methods;
Methods<vm2_get_station, vm2_get_variable> vm2_methods;
Methods<> odimh5_methods;
Methods<> nc_methods;
Methods<> jpeg_methods;

}

extern "C" {

static PyModuleDef scan_module = {
    PyModuleDef_HEAD_INIT,
    "scan",            /* m_name */
    "Arkimet format-specific functions",  /* m_doc */
    -1,                /* m_size */
    scan_methods.as_py(),  /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef scanners_module = {
    PyModuleDef_HEAD_INIT,
    "scanners",            /* m_name */
    "Arkimet scanner code",  /* m_doc */
    -1,                /* m_size */
    scanners_methods.as_py(),  /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef grib_module = {
    PyModuleDef_HEAD_INIT,
    "grib",            /* m_name */
    "Arkimet GRIB-specific functions",  /* m_doc */
    -1,                /* m_size */
    grib_methods.as_py(),  /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef bufr_module = {
    PyModuleDef_HEAD_INIT,
    "bufr",            /* m_name */
    "Arkimet BUFR-specific functions",  /* m_doc */
    -1,                /* m_size */
    bufr_methods.as_py(),  /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef odimh5_module = {
    PyModuleDef_HEAD_INIT,
    "odimh5",             /* m_name */
    "Arkimet ODIMh5-specific functions",  /* m_doc */
    -1,                /* m_size */
    odimh5_methods.as_py(),  /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef nc_module = {
    PyModuleDef_HEAD_INIT,
    "nc",             /* m_name */
    "Arkimet NetCDF-specific functions",  /* m_doc */
    -1,                /* m_size */
    nc_methods.as_py(),  /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef jpeg_module = {
    PyModuleDef_HEAD_INIT,
    "jpeg",            /* m_name */
    "Arkimet JPEG-specific functions",  /* m_doc */
    -1,                /* m_size */
    jpeg_methods.as_py(),  /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef vm2_module = {
    PyModuleDef_HEAD_INIT,
    "vm2",             /* m_name */
    "Arkimet VM2-specific functions",  /* m_doc */
    -1,                /* m_size */
    vm2_methods.as_py(),  /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

}

namespace arki {
namespace python {

void register_scan(PyObject* m)
{
#ifdef HAVE_DBALLE
    wreport_api.import();
    dballe_api.import();
#endif

    pyo_unique_ptr grib = throw_ifnull(PyModule_Create(&grib_module));
    grib_def = new GribDef;
    grib_def->define(arkipy_scan_Grib_Type, grib);

    pyo_unique_ptr bufr = throw_ifnull(PyModule_Create(&bufr_module));
    pyo_unique_ptr odimh5 = throw_ifnull(PyModule_Create(&odimh5_module));
    pyo_unique_ptr nc = throw_ifnull(PyModule_Create(&nc_module));
    pyo_unique_ptr jpeg = throw_ifnull(PyModule_Create(&jpeg_module));
    pyo_unique_ptr vm2 = throw_ifnull(PyModule_Create(&vm2_module));
    pyo_unique_ptr scan = throw_ifnull(PyModule_Create(&scan_module));
    pyo_unique_ptr scanners = throw_ifnull(PyModule_Create(&scanners_module));

    module_arkimet = m;
    module_scanners = scanners;

    if (PyModule_AddObject(scan, "grib", grib.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(scan, "bufr", bufr.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(scan, "odimh5", odimh5.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(scan, "nc", nc.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(scan, "jpeg", jpeg.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(scan, "vm2", vm2.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(scan, "scanners", scanners.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(m, "scan", scan.release()) == -1)
        throw PythonException();
}

namespace scan {
void init()
{
    arki::scan::Scanner::register_factory("grib", [] {
        return std::make_shared<PythonGribScanner>();
    });
#ifdef HAVE_DBALLE
    arki::scan::Scanner::register_factory("bufr", [] {
        return std::make_shared<PythonBufrScanner>();
    });
#endif
    arki::scan::Scanner::register_factory("odimh5", [] {
        return std::make_shared<PythonOdimh5Scanner>();
    });
    arki::scan::Scanner::register_factory("nc", [] {
        return std::make_shared<PythonNetCDFScanner>();
    });
    arki::scan::Scanner::register_factory("jpeg", [] {
        return std::make_shared<PythonJPEGScanner>();
    });
}
}

}
}
