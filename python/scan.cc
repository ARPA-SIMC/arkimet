#include "scan.h"
#include "common.h"
#include "structured.h"
#include "utils/methods.h"
#include "arki/values.h"
#include "arki/utils/vm2.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/runtime.h"
#include "arki/scan/grib.h"
#if 0
#include "metadata.h"
#include "summary.h"
#include "cfg.h"
#include "files.h"
#include "matcher.h"
#include "utils/values.h"
#include "utils/type.h"
#include "utils/dict.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/dataset.h"
#include "arki/dataset/http.h"
#include "arki/dataset/time.h"
#include "arki/dataset/segmented.h"
#include "arki/nag.h"
#include "dataset/reporter.h"
#include "arki/sort.h"
#include <vector>
#include "config.h"
#endif

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::python;

namespace {

/*
 * scan.grib module functions
 */

#if 0
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
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "GribScanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    gribscanner_object = obj.release();
}


class PythonGrbScanner : public arki::scan::GribScanner
{
protected:
    void scan(grib_handle* gh, Metadata& md) override
    {
        AcquireGIL gil;
        if (!gribscanner_object)
            load_gribscanner_object();

        pyo_unique_ptr obj(PyObject_CallMethod(
                        gribscanner_object, "scan", "OO", Py_None, e.res.get()));
    }

public:
    PythonGribScanner()
    {
    }
    virtual ~PythonGribScanner()
    {
    }

    std::unique_ptr<arki::utils::geos::Geometry> compute(const arki::types::Area& v) const override
    {

        arki::python::PythonEmitter e;
        v.serialise(e, arki::structured::keys_python);


        if (!obj)
        {
            PyObject *type, *value, *traceback;
            PyErr_Fetch(&type, &value, &traceback);
            pyo_unique_ptr exc_type(type);
            pyo_unique_ptr exc_value(value);
            pyo_unique_ptr exc_traceback(traceback);

            arki::nag::debug_tty("python bbox failed: %s", exc_type.str().c_str());
            arki::nag::warning("python bbox failed: %s", exc_type.str().c_str());
            return std::unique_ptr<arki::utils::geos::Geometry>();
        }

        if (obj == Py_None)
            return std::unique_ptr<arki::utils::geos::Geometry>();

        Py_ssize_t size = PyList_Size(obj);
        if (size == -1)
            throw PythonException();

        switch (size)
        {
            case 0: return std::unique_ptr<arki::utils::geos::Geometry>();
            case 1:
            {
                auto gf = arki::utils::geos::GeometryFactory::getDefaultInstance();
                PyObject* pair = PyList_GET_ITEM(obj.get(), 0); // Borrowed reference
                auto point = get_coord_pair(pair);
                return std::unique_ptr<arki::utils::geos::Geometry>(
                        gf->createPoint(arki::utils::geos::Coordinate(point.second, point.first)));
            }
            default:
            {
                auto gf = arki::utils::geos::GeometryFactory::getDefaultInstance();
                arki::utils::geos::CoordinateArraySequence cas;
                for (Py_ssize_t i = 0; i < size; ++i)
                {
                    PyObject* pair = PyList_GET_ITEM(obj.get(), i); // Borrowed reference
                    auto point = get_coord_pair(pair);
                    cas.add(arki::utils::geos::Coordinate(point.second, point.first));
                }
                std::unique_ptr<arki::utils::geos::LinearRing> lr(gf->createLinearRing(cas));
                return std::unique_ptr<arki::utils::geos::Geometry>(
                        gf->createPolygon(*lr, std::vector<arki::utils::geos::Geometry*>()));
            }
        }
    }
};
#endif

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
    pyo_unique_ptr vm2 = throw_ifnull(PyModule_Create(&vm2_module));
    pyo_unique_ptr scan = throw_ifnull(PyModule_Create(&scan_module));

    if (PyModule_AddObject(scan, "vm2", vm2.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(m, "scan", scan.release()) == -1)
        throw PythonException();
}

}
}
