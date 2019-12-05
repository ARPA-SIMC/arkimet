#ifndef ARKI_PYTHON_FORMATTER_H
#define ARKI_PYTHON_FORMATTER_H

#include "arki/metadata/fwd.h"
#include "python/utils/core.h"

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::Formatter* formatter;
} arkipy_Formatter;

extern PyTypeObject* arkipy_Formatter_Type;

#define arkipy_Formatter_Check(ob) \
    (Py_TYPE(ob) == arkipy_Formatter_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_Formatter_Type))

}

namespace arki {
namespace python {

void register_formatter(PyObject* m);

namespace formatter {

void init();

}
}
}

#endif
