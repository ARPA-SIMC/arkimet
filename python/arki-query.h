#ifndef ARKI_PYTHON_ARKI_QUERY_H
#define ARKI_PYTHON_ARKI_QUERY_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/core/cfg.h>
#include <memory>

namespace arki {
namespace runtime {
struct DatasetProcessor;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::core::cfg::Sections inputs;
    arki::runtime::DatasetProcessor* processor = nullptr;
} arkipy_ArkiQuery;

extern PyTypeObject* arkipy_ArkiQuery_Type;

#define arkipy_ArkiQuery_Check(ob) \
    (Py_TYPE(ob) == &arkipy_ArkiQuery_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_ArkiQuery_Type))
}

namespace arki {
namespace python {

void register_arki_query(PyObject* m);

}
}

#endif
