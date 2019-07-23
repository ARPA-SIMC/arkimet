#ifndef ARKI_PYTHON_ARKI_CMDLINE_H
#define ARKI_PYTHON_ARKI_CMDLINE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <memory>

namespace arki {
namespace python {

namespace cmdline {
struct DatasetProcessor;
}

/**
 * Build a DatasetProcessor from the given python function args/kwargs
 */
std::unique_ptr<cmdline::DatasetProcessor> build_processor(PyObject* args, PyObject* kw);

bool foreach_stdin(const std::string& format, std::function<void(dataset::Reader&)> dest);
bool foreach_sections(const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest);

}
}

#endif
