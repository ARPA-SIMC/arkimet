#include "progress.h"
#include "arki/exceptions.h"
#include "python/utils/values.h"

namespace arki {
namespace python {
namespace dataset {

void PythonProgress::call_update()
{
    pyo_unique_ptr py_count(to_python(partial_count));
    pyo_unique_ptr py_bytes(to_python(partial_bytes));
    pyo_unique_ptr args(throw_ifnull(PyTuple_Pack(2, py_count.get(), py_bytes.get())));
    pyo_unique_ptr res(throw_ifnull(PyObject_Call(meth_update, args.get(), nullptr)));
    partial_count = 0;
    partial_bytes = 0;
}

PythonProgress::PythonProgress(PyObject* progress)
{
    if (progress and progress != Py_None)
    {
        meth_start = throw_ifnull(PyObject_GetAttrString(progress, "start"));
        meth_update = throw_ifnull(PyObject_GetAttrString(progress, "update"));
        meth_done = throw_ifnull(PyObject_GetAttrString(progress, "done"));
    }
}

PythonProgress::~PythonProgress()
{
    AcquireGIL gil;
    Py_XDECREF(meth_done);
    Py_XDECREF(meth_update);
    Py_XDECREF(meth_start);
}

void PythonProgress::start(size_t expected_count, size_t expected_bytes)
{
    arki::dataset::QueryProgress::start(expected_count, expected_bytes);

    if (meth_start)
    {
        AcquireGIL gil;
        pyo_unique_ptr count(to_python(expected_count));
        pyo_unique_ptr bytes(to_python(expected_bytes));
        pyo_unique_ptr args(throw_ifnull(PyTuple_Pack(2, count.get(), bytes.get())));
        pyo_unique_ptr res(throw_ifnull(PyObject_Call(meth_start, args.get(), nullptr)));
    }
}

void PythonProgress::update(size_t count, size_t bytes)
{
    arki::dataset::QueryProgress::update(count, bytes);

    partial_count += count;
    partial_bytes += bytes;

    // Don't trigger more than once every 0.2 seconds
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC_COARSE, &now) == -1)
        throw_system_error("clock_gettime failed");
    int diff = (now.tv_sec - last_call.tv_sec) * 1000 + (now.tv_nsec - last_call.tv_nsec) / 1000000;
    if (diff < 200)
        return;
    last_call = now;

    AcquireGIL gil;
    if (PyErr_CheckSignals() == -1)
        throw PythonException();
    if (meth_update)
        call_update();
}

void PythonProgress::done()
{
    arki::dataset::QueryProgress::done();

    AcquireGIL gil;
    if (meth_update and (partial_count or partial_bytes))
        call_update();

    if (meth_done)
    {
        pyo_unique_ptr py_count(to_python(count));
        pyo_unique_ptr py_bytes(to_python(bytes));
        pyo_unique_ptr args(throw_ifnull(PyTuple_Pack(2, py_count.get(), py_bytes.get())));
        pyo_unique_ptr res(throw_ifnull(PyObject_Call(meth_done, args.get(), nullptr)));
    }
}

}
}
}
