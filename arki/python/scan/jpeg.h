#ifndef ARKI_PYTHON_SCAN_JPEG_H
#define ARKI_PYTHON_SCAN_JPEG_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

namespace arki::python::scan {

void register_scan_jpeg(PyObject* scan);
void init_scanner_jpeg();

} // namespace arki::python::scan

#endif
