#ifndef ARKI_PYTHON_DATASET_PYTHON_H
#define ARKI_PYTHON_DATASET_PYTHON_H

#include "arki/dataset.h"
#include "python/utils/core.h"


namespace arki {
namespace python {
namespace dataset {

std::shared_ptr<arki::dataset::Reader> create_reader(const arki::core::cfg::Section& cfg, PyObject* o);
std::shared_ptr<arki::dataset::Writer> create_writer(PyObject* o);
std::shared_ptr<arki::dataset::Checker> create_checker(PyObject* o);

}
}
}

#endif
