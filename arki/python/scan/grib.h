#ifndef ARKI_PYTHON_SCAN_GRIB_H
#define ARKI_PYTHON_SCAN_GRIB_H

#define PY_SSIZE_T_CLEAN
#include "arki/data/grib.h"
#include <Python.h>

extern "C" {

typedef struct
{
    PyObject_HEAD;
    grib_handle* gh;
    bool owns_handle;
} arkipy_scan_Grib;

extern PyTypeObject* arkipy_scan_Grib_Type;

#define arkipy_scan_Grib_Check(ob)                                             \
    (Py_TYPE(ob) == arkipy_scan_Grib_Type ||                                   \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_scan_Grib_Type))

typedef struct
{
    PyObject_HEAD;
    FILE* file;
    grib_context* context;
} arkipy_scan_GribReader;

extern PyTypeObject* arkipy_scan_GribReader_Type;

#define arkipy_scan_GribReader_Check(ob)                                       \
    (Py_TYPE(ob) == arkipy_scan_GribReader_Type ||                             \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_scan_GribReader_Type))
}

namespace arki::python::scan {

void register_scan_grib(PyObject* scan);
void init_scanner_grib();

} // namespace arki::python::scan

#endif
