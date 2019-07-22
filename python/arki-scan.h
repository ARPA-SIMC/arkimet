#ifndef ARKI_PYTHON_ARKI_SCAN_H
#define ARKI_PYTHON_ARKI_SCAN_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/core/cfg.h>
#include <memory>

namespace arki {
namespace runtime {
struct DatasetProcessor;
}

namespace python {
namespace arki_scan {
struct MetadataDispatch;
}
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::core::cfg::Sections inputs;
    arki::runtime::DatasetProcessor* processor = nullptr;
    arki::python::arki_scan::MetadataDispatch* dispatcher = nullptr;
} arkipy_ArkiScan;

extern PyTypeObject* arkipy_ArkiScan_Type;

#define arkipy_ArkiScan_Check(ob) \
    (Py_TYPE(ob) == &arkipy_ArkiScan_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_ArkiScan_Type))
}

namespace arki {
namespace python {

void register_arki_scan(PyObject* m);

}
}

#endif
