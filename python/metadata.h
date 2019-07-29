#ifndef ARKI_PYTHON_METADATA_H
#define ARKI_PYTHON_METADATA_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/defs.h>
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


typedef struct {
    PyObject_HEAD
    arki::metadata_dest_func func;
} arkipy_metadata_dest_func;

extern PyTypeObject* arkipy_metadata_dest_func_Type;

#define arkipy_metadata_dest_func_Check(ob) \
    (Py_TYPE(ob) == arkipy_metadata_dest_func_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_metadata_dest_func_Type))

}

namespace arki {
namespace python {

/**
 * Return a metadata::Collection from a python sequence of arkimet.Metadata
 * objects
 */
arki::metadata::Collection metadata_collection_from_python(PyObject* o);

arkipy_Metadata* metadata_create(std::unique_ptr<Metadata>&& md);


/**
 * Create a metadata_dest_func from a python object.
 *
 * Note that the object should not be destroyed during the lifetime of the
 * resulting function.
 */
arki::metadata_dest_func dest_func_from_python(PyObject* o);

/// Create a python callable that proxies to a metadata_dest_func
PyObject* dest_func_to_python(arki::metadata_dest_func func);

inline PyObject* to_python(arki::metadata_dest_func func) { return dest_func_to_python(func); }


void register_metadata(PyObject* m);

}
}
#endif

