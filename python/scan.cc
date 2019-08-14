#include "scan.h"
#include "common.h"
#include "structured.h"
#include "metadata.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "arki/metadata.h"
#include "arki/values.h"
#include "arki/utils/vm2.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/runtime.h"
#include "arki/scan/grib.h"
#include "arki/scan/bufr.h"
#include "arki/nag.h"
#include <grib_api.h>
#include <dballe/message.h>
#include <dballe/var.h>
#include <wreport/python.h>


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

// Index to the wreport python API
wrpy_c_api* wrpy = 0;

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

arkipy_scan_BufrMessage* bufrmessage_create(dballe::Message& msg)
{
    arkipy_scan_BufrMessage* result = PyObject_New(arkipy_scan_BufrMessage, arkipy_scan_BufrMessage_Type);
    if (!result) throw PythonException();
    result->msg = &msg;
    return result;
}

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
    void scan_extra(dballe::Message& msg, std::shared_ptr<Metadata> md) override
    {
        auto orig_use_count = md.use_count();

        AcquireGIL gil;
        if (!bufrscanner_object)
            load_bufrscanner_object();

        pyo_unique_ptr pymsg((PyObject*)bufrmessage_create(msg));
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


// FIXME: implement exporting a capsule with API functions in dballe, and use it here

struct msg_type : public Getter<msg_type, arkipy_scan_BufrMessage>
{
    constexpr static const char* name = "type";
    constexpr static const char* doc = "return the BUFR message type";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try {
            std::string name = str::lower(dballe::format_message_type(self->msg->get_type()));
            return to_python(name);
        } ARKI_CATCH_RETURN_PYO;
    }
};

struct msg_get_named : public MethKwargs<msg_get_named, arkipy_scan_BufrMessage>
{
    constexpr static const char* name = "get_named";
    constexpr static const char* signature = "name: str";
    constexpr static const char* returns = "Optional[dballe.Var]";
    constexpr static const char* summary = "return the variable given its shortcut name, or None of not found";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "name", NULL };
        const char* name = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s", (char**)kwlist, &name))
            return nullptr;

        try {
            const wreport::Var* res = self->msg->get(name);
            if (!res)
                Py_RETURN_NONE;
            else
                return (PyObject*)wrpy->var_create_copy(*res);
        } ARKI_CATCH_RETURN_PYO
    }
};

wreport::Varcode varcode_from_python(PyObject* o)
{
    return dballe::resolve_varcode(from_python<std::string>(o));
}

struct msg_get : MethKwargs<msg_get, arkipy_scan_BufrMessage>
{
    constexpr static const char* name = "get";
    constexpr static const char* signature = "level: dballe.Level, trange: dballe.Trange, code: str";
    constexpr static const char* returns = "Optional[dballe.Var]";
    constexpr static const char* summary = "Get a Var given its level, timerange, and varcode; returns None if not found";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "level", "trange", "code", nullptr };
        PyObject* pylevel = nullptr;
        PyObject* pytrange = nullptr;
        PyObject* pycode = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OOO", const_cast<char**>(kwlist), &pylevel, &pytrange, &pycode))
            return nullptr;

        try {
            if (pylevel && pylevel != Py_None)
            {
                PyErr_SetString(PyExc_NotImplementedError, "dballe message lookup by level is not implemented");
                throw PythonException();
            }
            if (pytrange && pytrange != Py_None)
            {
                PyErr_SetString(PyExc_NotImplementedError, "dballe message lookup by timerange is not implemented");
                throw PythonException();
            }
            dballe::Level level;
            dballe::Trange trange;
            wreport::Varcode code = varcode_from_python(pycode);

            const wreport::Var* res = self->msg->get(level, trange, code);
            if (!res)
                Py_RETURN_NONE;
            else
                return (PyObject*)wrpy->var_create_copy(*res);
        } ARKI_CATCH_RETURN_PYO
    }
};

PyObject* varcode_to_python(wreport::Varcode code)
{
    char buf[7];
    snprintf(buf, 7, "%c%02d%03d",
            WR_VAR_F(code) == 0 ? 'B' :
            WR_VAR_F(code) == 1 ? 'R' :
            WR_VAR_F(code) == 2 ? 'C' :
            WR_VAR_F(code) == 3 ? 'D' : '?',
            WR_VAR_X(code), WR_VAR_Y(code));
    return throw_ifnull(PyUnicode_FromString(buf));
}


// TODO: implement in python when we can use dballe's dballe.Message
struct msg_get_pollution_type : public MethNoargs<msg_get_pollution_type, arkipy_scan_BufrMessage>
{
    constexpr static const char* name = "get_pollution_type";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Optional[str]";
    constexpr static const char* summary = "get the pollution varcode for message, if present, or None otherwise";

    static PyObject* run(Impl* self)
    {
        try {
            wreport::Varcode code = 0;
            self->msg->foreach_var([&](const dballe::Level& level, const dballe::Trange& trange, const wreport::Var& var) {
                if (trange.is_missing())
                    return true;
                code = var.code();
                return false;
            });

            if (code != 0)
                return varcode_to_python(code);
            else
                Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct BufrMessageDef : public Type<BufrMessageDef, arkipy_scan_BufrMessage>
{
    constexpr static const char* name = "BufrMessage";
    constexpr static const char* qual_name = "arkimet.scan.grib.BufrMessage";
    constexpr static const char* doc = R"(
Access grib message contents
)";
    GetSetters<msg_type> getsetters;
    Methods<msg_get_named, msg_get, msg_get_pollution_type> methods;

    static void _dealloc(Impl* self)
    {
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString("BufrMessage");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromString("BufrMessage");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        // BufrMessage() should not be invoked as a constructor
        PyErr_SetString(PyExc_NotImplementedError, "BufrMessage objects cannot be constructed explicitly");
        return -1;
    }
};

BufrMessageDef* bufrmessage_def = nullptr;


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
            arki::ValueBag values(arki::utils::vm2::get_station(id));
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
            arki::ValueBag values(arki::utils::vm2::get_variable(id));
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
    if (!wrpy)
    {
        pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("wreport")));

        wrpy = (wrpy_c_api*)PyCapsule_Import("_wreport._C_API", 0);
        if (!wrpy)
            throw PythonException();

#if 0
        // TODO: uncomment when the new wreport has been widely deployed
        if (wrpy->version_major != 1)
        {
            PyErr_Format(PyExc_RuntimeError, "wreport C API version is %d.%d but only 1.x is supported", wrpy->version_major, wrpy->version_minor);
            throw PythonException();
        }
#endif
    }

    pyo_unique_ptr grib = throw_ifnull(PyModule_Create(&grib_module));
    grib_def = new GribDef;
    grib_def->define(arkipy_scan_Grib_Type, grib);

    pyo_unique_ptr bufr = throw_ifnull(PyModule_Create(&bufr_module));
    bufrmessage_def = new BufrMessageDef;
    bufrmessage_def->define(arkipy_scan_BufrMessage_Type, bufr);

    pyo_unique_ptr vm2 = throw_ifnull(PyModule_Create(&vm2_module));
    pyo_unique_ptr scan = throw_ifnull(PyModule_Create(&scan_module));
    pyo_unique_ptr scanners = throw_ifnull(PyModule_Create(&scanners_module));

    module_arkimet = m;
    module_scanners = scanners;

    if (PyModule_AddObject(scan, "grib", grib.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(scan, "bufr", bufr.release()) == -1)
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
        return std::unique_ptr<arki::scan::Scanner>(new PythonGribScanner);
    });
    arki::scan::Scanner::register_factory("bufr", [] {
        return std::unique_ptr<arki::scan::Scanner>(new PythonBufrScanner);
    });
}
}

}
}
