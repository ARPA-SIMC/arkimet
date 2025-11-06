#ifndef ARKI_PYTHON_SCAN_BUFR_H
#define ARKI_PYTHON_SCAN_BUFR_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

namespace arki::python::scan {

void register_scan_bufr(PyObject* scan);
void init_scanner_bufr();

} // namespace arki::python::scan

#endif
