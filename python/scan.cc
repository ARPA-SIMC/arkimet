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
#include <grib_api.h>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_scan_Grib_Type = nullptr;

}

namespace {

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
    using namespace arki;
    std::vector<std::string> sources = arki::Config::get().dir_scan_grib.list_files(".py");
    for (const auto& source: sources)
    {
        std::string basename = str::basename(source);

        // Check if the bbox module had already been imported
        std::string module_name = "arkimet.scan.grib." + basename.substr(0, basename.size() - 3);
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
    void scan(grib_handle* gh, std::shared_ptr<Metadata> md) override
    {
        AcquireGIL gil;
        if (!gribscanner_object)
            load_gribscanner_object();

        pyo_unique_ptr pygh((PyObject*)grib_create(gh));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(PyObject_CallMethod(
                        gribscanner_object, "scan", "OO", pygh.get(), pymd.get()));
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

#if 0

// Lookup a grib value for grib.<fieldname>
int GribLua::arkilua_lookup_gribs(lua_State* L)
{
    GribLua& s = get_griblua(L, 1, "gribs");
    grib_handle* gh = s.gh;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, 2);
	const char* name = lua_tostring(L, 2);

	// Push the function result for lua
	const int maxsize = 1000;
	char buf[maxsize];
	size_t len = maxsize;
	int res = grib_get_string(gh, name, buf, &len);
	if (res == GRIB_NOT_FOUND)
	{
		lua_pushnil(L);
	} else {
		arkilua_check_gribapi(L, res, "reading string value");
		if (len > 0) --len;
		lua_pushlstring(L, buf, len);
	}

	// Number of values we're returning to lua
	return 1;
}

// Lookup a grib value for grib.<fieldname>
int GribLua::arkilua_lookup_gribd(lua_State* L)
{
    GribLua& s = get_griblua(L, 1, "gribd");
    grib_handle* gh = s.gh;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, 2);
	const char* name = lua_tostring(L, 2);

	// Push the function result for lua
	double val;
	int res = grib_get_double(gh, name, &val);
	if (res == GRIB_NOT_FOUND)
	{
		lua_pushnil(L);
	} else {
		arkilua_check_gribapi(L, res, "reading double value");
		lua_pushnumber(L, val);
	}

	// Number of values we're returning to lua
	return 1;
}
#endif

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
Methods<> grib_methods;
Methods<vm2_get_station, vm2_get_variable> vm2_methods;

}

extern "C" {

static PyModuleDef scan_module = {
    PyModuleDef_HEAD_INIT,
    "scan",         /* m_name */
    "Arkimet format-specific functions",  /* m_doc */
    -1,             /* m_size */
    scan_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef grib_module = {
    PyModuleDef_HEAD_INIT,
    "grib",         /* m_name */
    "Arkimet GRIB-specific functions",  /* m_doc */
    -1,             /* m_size */
    grib_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef vm2_module = {
    PyModuleDef_HEAD_INIT,
    "vm2",         /* m_name */
    "Arkimet VM2-specific functions",  /* m_doc */
    -1,             /* m_size */
    vm2_methods.as_py(),    /* m_methods */
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
    pyo_unique_ptr grib = throw_ifnull(PyModule_Create(&grib_module));
    grib_def = new GribDef;
    grib_def->define(arkipy_scan_Grib_Type, grib);

    pyo_unique_ptr vm2 = throw_ifnull(PyModule_Create(&vm2_module));
    pyo_unique_ptr scan = throw_ifnull(PyModule_Create(&scan_module));

    if (PyModule_AddObject(scan, "grib", grib.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(scan, "vm2", vm2.release()) == -1)
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
}
}

}
}
