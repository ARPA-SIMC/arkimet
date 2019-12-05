#ifndef ARKI_PYTHON_BBOX_H
#define ARKI_PYTHON_BBOX_H

#include "arki/metadata/fwd.h"
#include "python/utils/core.h"

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::BBox* bbox;
} arkipy_BBox;

extern PyTypeObject* arkipy_BBox_Type;

#define arkipy_BBox_Check(ob) \
    (Py_TYPE(ob) == arkipy_BBox_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_BBox_Type))

}

namespace arki {
namespace python {

void register_bbox(PyObject* m);

namespace bbox {

void init();

}
}
}

#endif

