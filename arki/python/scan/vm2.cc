#include "vm2.h"
#include "arki/python/common.h"
#include "arki/python/structured.h"
#include "arki/python/utils/methods.h"
#include "arki/utils/vm2.h"

namespace arki::python::scan {

/*
 * scan.vm2 module functions
 */

struct vm2_get_station : public MethKwargs<vm2_get_station, PyObject>
{
    constexpr static const char* name      = "get_station";
    constexpr static const char* signature = "id: int";
    constexpr static const char* returns   = "Dict[str, Any]";
    constexpr static const char* summary =
        "Read the station attributes for a VM2 station ID";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"id", nullptr};
        int id;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "i", pass_kwlist(kwlist),
                                         &id))
            return nullptr;

        try
        {
            arki::types::ValueBag values(arki::utils::vm2::get_station(id));
            arki::python::PythonEmitter e;
            values.serialise(e);
            return e.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct vm2_get_variable : public MethKwargs<vm2_get_variable, PyObject>
{
    constexpr static const char* name      = "get_variable";
    constexpr static const char* signature = "id: int";
    constexpr static const char* returns   = "Dict[str, Any]";
    constexpr static const char* summary =
        "Read the variable attributes for a VM2 variable ID";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"id", nullptr};
        int id;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "i", pass_kwlist(kwlist),
                                         &id))
            return nullptr;

        try
        {
            arki::types::ValueBag values(arki::utils::vm2::get_variable(id));
            arki::python::PythonEmitter e;
            values.serialise(e);
            return e.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

Methods<vm2_get_station, vm2_get_variable> vm2_methods;

extern "C" {

static PyModuleDef vm2_module = {
    PyModuleDef_HEAD_INIT,
    "vm2",                            /* m_name */
    "Arkimet VM2-specific functions", /* m_doc */
    -1,                               /* m_size */
    vm2_methods.as_py(),              /* m_methods */
    nullptr,                          /* m_slots */
    nullptr,                          /* m_traverse */
    nullptr,                          /* m_clear */
    nullptr,                          /* m_free */
};
}

void register_scan_vm2(PyObject* scan)
{
    pyo_unique_ptr vm2 = throw_ifnull(PyModule_Create(&vm2_module));

    if (PyModule_AddObject(scan, "vm2", vm2.release()) == -1)
        throw PythonException();
}
} // namespace arki::python::scan
