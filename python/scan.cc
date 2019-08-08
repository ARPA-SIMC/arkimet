#include "scan.h"
#include "common.h"
#include "structured.h"
#include "utils/methods.h"
#include "arki/values.h"
#include "arki/utils/vm2.h"
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
using namespace arki::python;

namespace {

/*
 * scan.vm2 module functions
 */

struct get_station : public MethKwargs<get_station, PyObject>
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

Methods<> scan_methods;
Methods<get_station> vm2_methods;

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
