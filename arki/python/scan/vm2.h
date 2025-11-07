#ifndef ARKI_PYTHON_SCAN_VM2_H
#define ARKI_PYTHON_SCAN_VM2_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

namespace arki::python::scan {

void register_scan_vm2(PyObject* scan);

}

#endif
