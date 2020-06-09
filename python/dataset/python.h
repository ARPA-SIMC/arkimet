#ifndef ARKI_PYTHON_DATASET_PYTHON_H
#define ARKI_PYTHON_DATASET_PYTHON_H

#include "arki/dataset.h"
#include "python/utils/core.h"


namespace arki {
namespace python {
namespace dataset {

std::shared_ptr<arki::dataset::Reader> create_reader(std::shared_ptr<arki::dataset::Session> session, PyObject* o);
std::shared_ptr<arki::dataset::Writer> create_writer(std::shared_ptr<arki::dataset::Session> session, PyObject* o);
std::shared_ptr<arki::dataset::Checker> create_checker(std::shared_ptr<arki::dataset::Session> session, PyObject* o);

}
}
}

#endif
