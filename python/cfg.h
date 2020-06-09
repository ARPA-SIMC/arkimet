#ifndef ARKI_PYTHON_CFG_H
#define ARKI_PYTHON_CFG_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>
#include "arki/core/cfg.h"
#include "python/common.h"

namespace arki {
namespace runtime {
struct ArkiMergeconf;
}
}

extern "C" {

typedef arki::python::SharedPtrWrapper<arki::core::cfg::Sections> arkipy_cfgSections;
extern PyTypeObject* arkipy_cfgSections_Type;

#define arkipy_cfgSections_Check(ob) \
    (Py_TYPE(ob) == arkipy_cfgSections_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_cfgSections_Type))


typedef arki::python::SharedPtrWrapper<arki::core::cfg::Section> arkipy_cfgSection;
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
std::shared_ptr<core::cfg::Section> section_from_python(PyObject* o);

/**
 * Create a cfg::Sections from python.
 *
 * Currently this only supports:
 *  - str or bytes, that will get parsed
 */
std::shared_ptr<core::cfg::Sections> sections_from_python(PyObject* o);

/**
 * Pass a mutable reference to a Sections to Python
 */
PyObject* sections_to_python(std::shared_ptr<core::cfg::Sections> ptr);
inline PyObject* to_python(std::shared_ptr<core::cfg::Sections> ptr) { return sections_to_python(ptr); }

/**
 * Pass a mutable reference to a Section to Python
 */
PyObject* section_to_python(std::shared_ptr<core::cfg::Section> ptr);
inline PyObject* to_python(std::shared_ptr<core::cfg::Section> ptr) { return section_to_python(ptr); }

void register_cfg(PyObject* m);

}
}

#endif

