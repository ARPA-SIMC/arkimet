#ifndef ARKI_PYTHON_SCAN_GRIB_H
#define ARKI_PYTHON_SCAN_GRIB_H

#define PY_SSIZE_T_CLEAN
#include "arki/scan/grib.h"
#include <Python.h>

extern "C" {

typedef struct
{
    PyObject_HEAD grib_handle* gh;
} arkipy_scan_Grib;

extern PyTypeObject* arkipy_scan_Grib_Type;

#define arkipy_scan_Grib_Check(ob)                                             \
    (Py_TYPE(ob) == arkipy_scan_Grib_Type ||                                   \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_scan_Grib_Type))
}

namespace arki::python::scan {

void register_scan_grib(PyObject* scan);
void init_scanner_grib();

} // namespace arki::python::scan

#endif
