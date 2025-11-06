#ifndef ARKI_PYTHON_SCAN_ODIMH5_H
#define ARKI_PYTHON_SCAN_ODIMH5_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

namespace arki::python::scan {

void register_scan_odimh5(PyObject* scan);
void init_scanner_odimh5();

} // namespace arki::python::scan

#endif
