#ifndef ARKI_PYTHON_ARKI_MERGECONF_H
#define ARKI_PYTHON_ARKI_MERGECONF_H

#include <Python.h>
#include <memory>

namespace arki {
namespace runtime {
struct ArkiMergeconf;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::runtime::ArkiMergeconf* arki_mergeconf;
} arkipy_ArkiMergeconf;

extern PyTypeObject* arkipy_ArkiMergeconf_Type;

#define arkipy_ArkiMergeconf_Check(ob) \
    (Py_TYPE(ob) == &arkipy_ArkiMergeconf_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_ArkiMergeconf_Type))
}

namespace arki {
namespace python {

void register_arki_mergeconf(PyObject* m);

}
}

#endif
