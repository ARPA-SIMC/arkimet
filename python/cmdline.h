#ifndef ARKI_PYTHON_ARKI_CMDLINE_H
#define ARKI_PYTHON_ARKI_CMDLINE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <memory>

namespace arki {

namespace runtime {
struct DatasetProcessor;
struct MetadataDispatch;
}

namespace python {

/**
 * Build a DatasetProcessor from the given python function args/kwargs
 */
std::unique_ptr<runtime::DatasetProcessor> build_processor(PyObject* args, PyObject* kw);

/**
 * Build a MetadataDispatcher from the given python function args/kwargs
 */
std::unique_ptr<runtime::MetadataDispatch> build_dispatcher(runtime::DatasetProcessor& processor, PyObject* args, PyObject* kw);

}
}

#endif
