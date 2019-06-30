#ifndef ARKI_PYTHON_CFG_H
#define ARKI_PYTHON_CFG_H

#include <Python.h>
#include <memory>
#include "arki/core/cfg.h"

namespace arki {
namespace runtime {
struct ArkiMergeconf;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::core::cfg::Sections sections;
} arkipy_cfgSections;

extern PyTypeObject* arkipy_cfgSections_Type;

#define arkipy_cfgSections_Check(ob) \
    (Py_TYPE(ob) == &arkipy_cfgSections_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_cfgSections_Type))


typedef struct {
    PyObject_HEAD
    PyObject* owner;
    arki::core::cfg::Section* section;
} arkipy_cfgSection;

extern PyTypeObject* arkipy_cfgSection_Type;

#define arkipy_cfgSection_Check(ob) \
    (Py_TYPE(ob) == &arkipy_cfgSection_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &arkipy_cfgSection_Type))

}

namespace arki {
namespace python {

void register_cfg(PyObject* m);

}
}

#endif

