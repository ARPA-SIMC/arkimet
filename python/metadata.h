#ifndef ARKI_PYTHON_METADATA_H
#define ARKI_PYTHON_METADATA_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/metadata/fwd.h>
#include <memory>

extern "C" {

typedef struct {
    PyObject_HEAD
    arki::Metadata* md;
} arkipy_Metadata;

extern PyTypeObject* arkipy_Metadata_Type;

#define arkipy_Metadata_Check(ob) \
    (Py_TYPE(ob) == arkipy_Metadata_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_Metadata_Type))
}

namespace arki {
namespace python {

/**
 * Return a metadata::Collection from a python sequence of arkimet.Metadata
 * objects
 */
arki::metadata::Collection metadata_collection_from_python(PyObject* o);

arkipy_Metadata* metadata_create(std::unique_ptr<Metadata>&& md);

void register_metadata(PyObject* m);

}
}
#endif

