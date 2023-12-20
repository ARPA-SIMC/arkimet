#ifndef ARKI_PYTHON_SCAN_H
#define ARKI_PYTHON_SCAN_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/scan/fwd.h>
#include <memory>

namespace dballe {
struct Message;
}

extern "C" {

struct grib_handle;

typedef struct {
    PyObject_HEAD
    grib_handle* gh;
} arkipy_scan_Grib;

extern PyTypeObject* arkipy_scan_Grib_Type;

#define arkipy_scan_Grib_Check(ob) \
    (Py_TYPE(ob) == arkipy_scan_Grib_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_scan_Grib_Type))


typedef struct {
    PyObject_HEAD
    std::shared_ptr<arki::scan::Scanner> scanner;
} arkipy_scan_Scanner;

extern PyTypeObject* arkipy_scan_Scanner_Type;

#define arkipy_scan_Scanner_Check(ob) \
    (Py_TYPE(ob) == arkipy_scan_Scanner_Type || \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_scan_Scanner_Type))


}

namespace arki {
namespace python {

void register_scan(PyObject* m);

namespace scan {
void init();
arkipy_scan_Scanner* scanner_create(std::shared_ptr<arki::scan::Scanner> scanner);
}

}
}
#endif

