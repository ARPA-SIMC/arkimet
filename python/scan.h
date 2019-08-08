#ifndef ARKI_PYTHON_SCAN_H
#define ARKI_PYTHON_SCAN_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/scan/fwd.h>
#include <memory>

extern "C" {
}

namespace arki {
namespace python {

void register_scan(PyObject* m);

}
}
#endif

