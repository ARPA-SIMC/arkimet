#ifndef ARKI_PYTHON_DATASET_PROGRESS_H
#define ARKI_PYTHON_DATASET_PROGRESS_H

#include "arki/dataset/progress.h"
#include "python/utils/core.h"

namespace arki {
namespace python {
namespace dataset {

class PythonProgress: public arki::dataset::QueryProgress
{
protected:
    struct timespec last_call = { 0, 0 };
    PyObject* meth_start = nullptr;
    PyObject* meth_update = nullptr;
    PyObject* meth_done = nullptr;
    size_t partial_count = 0;
    size_t partial_bytes = 0;

    void call_update();

public:
    PythonProgress(PyObject* progress=nullptr);
    ~PythonProgress();

    void start(size_t expected_count=0, size_t expected_bytes=0) override;
    void update(size_t count, size_t bytes) override;
    void done() override;
};

}
}
}

#endif
