#ifndef ARKI_PYTHON_CFG_H
#define ARKI_PYTHON_CFG_H

#define PY_SSIZE_T_CLEAN
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
    (Py_TYPE(ob) == arkipy_cfgSections_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_cfgSections_Type))


typedef struct {
    PyObject_HEAD
    PyObject* owner;
    arki::core::cfg::Section* section;
} arkipy_cfgSection;

extern PyTypeObject* arkipy_cfgSection_Type;

#define arkipy_cfgSection_Check(ob) \
    (Py_TYPE(ob) == arkipy_cfgSection_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_cfgSection_Type))

}

namespace arki {
namespace python {

/**
 * Create a cfg::Section from python.
 *
 * Currently this only supports:
 *  - str or bytes, that will get parsed
 *  - dict, that will be set as key->val into out
 */
core::cfg::Section section_from_python(PyObject* o);

/**
 * Create a cfg::Sections from python.
 *
 * Currently this only supports:
 *  - str or bytes, that will get parsed
 */
core::cfg::Sections sections_from_python(PyObject* o);

PyObject* cfg_sections(const core::cfg::Sections& sections);
PyObject* cfg_sections(core::cfg::Sections&& sections);
PyObject* cfg_section(const core::cfg::Section& section);
PyObject* cfg_section(core::cfg::Section&& section);
PyObject* cfg_section_reference(PyObject* owner, core::cfg::Section* section);

void register_cfg(PyObject* m);

}
}

#endif

