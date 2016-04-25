#ifndef ARKI_PYTHON_DATASET_H
#define ARKI_PYTHON_DATASET_H

#include <Python.h>

namespace arki {
namespace dataset {
struct Reader;
}
}

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::dataset::Reader* ds;
} dpy_DatasetReader;

PyAPI_DATA(PyTypeObject) dpy_DatasetReader_Type;

#define dpy_DatasetReader_Check(ob) \
    (Py_TYPE(ob) == &dpy_DatasetReader_Type || \
     PyType_IsSubtype(Py_TYPE(ob), &dpy_DatasetReader_Type))
}

namespace arki {
namespace python {

dpy_DatasetReader* dataset_reader_create();

void register_dataset(PyObject* m);

}
}
#endif
