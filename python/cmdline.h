#ifndef ARKI_PYTHON_ARKI_CMDLINE_H
#define ARKI_PYTHON_ARKI_CMDLINE_H

#define PY_SSIZE_T_CLEAN
#include "python/files.h"
#include <Python.h>
#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <memory>

namespace arki {
namespace python {

namespace cmdline {
class DatasetProcessor;
}

/**
 * Build a DatasetProcessor from the given python function args/kwargs
 */
std::unique_ptr<cmdline::DatasetProcessor>
build_processor(std::shared_ptr<arki::dataset::Pool> pool, PyObject* args,
                PyObject* kw);

/**
 * Return true if all files were processed without exceptions. Returns false if
 * an exception was raised while processing some input
 */
bool foreach_file(std::shared_ptr<arki::dataset::Session> session,
                  BinaryInputFile& file, DataFormat format,
                  std::function<void(dataset::Reader&)> dest);

/**
 * Return true if all sections were processed without exceptions. Returns false
 * if an exception was raised while processing some input
 */
bool foreach_sections(std::shared_ptr<arki::dataset::Pool> pool,
                      std::function<void(dataset::Reader&)> dest);

} // namespace python
} // namespace arki

#endif
