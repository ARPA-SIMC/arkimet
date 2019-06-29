#ifndef ARKI_PYTHON_ARKI_QUERY_H
#define ARKI_PYTHON_ARKI_QUERY_H

#include <Python.h>
#include <memory>

namespace arki {
namespace runtime {
struct ArkiQuery;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::runtime::ArkiQuery* arki_query;
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
