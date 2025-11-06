#ifndef ARKI_PYTHON_SCAN_NETCDF_H
#define ARKI_PYTHON_SCAN_NETCDF_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

namespace arki::python::scan {

void register_scan_netcdf(PyObject* scan);
void init_scanner_netcdf();

} // namespace arki::python::scan

#endif
