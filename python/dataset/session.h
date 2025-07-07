#ifndef ARKI_PYTHON_DATASET_SESSION_H
#define ARKI_PYTHON_DATASET_SESSION_H

#define PY_SSIZE_T_CLEAN
#include "python/common.h"
#include "python/utils/type.h"
#include <Python.h>
#include <arki/dataset/fwd.h>
#include <memory>

extern "C" {

typedef struct
{
    PyObject_HEAD std::shared_ptr<arki::dataset::Session> session;
    std::shared_ptr<arki::dataset::Pool> pool;

    PyObject* python__exit__()
    {
        try
        {
            pool.reset();
            session.reset();
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
} arkipy_DatasetSession;
extern PyTypeObject* arkipy_DatasetSession_Type;

#define arkipy_DatasetSession_Check(ob)                                        \
    (Py_TYPE(ob) == arkipy_DatasetSession_Type ||                              \
     PyType_IsSubtype(Py_TYPE(ob), arkipy_DatasetSession_Type))
}

namespace arki {
namespace python {

arkipy_DatasetSession*
dataset_session_create(std::shared_ptr<arki::dataset::Session> session,
                       std::shared_ptr<arki::dataset::Pool> pool);

/**
 * Return a arki::dataset::Session from a Python object that contains it
 */
std::shared_ptr<arki::dataset::Session> session_from_python(PyObject* o);

/**
 * Return a arki::dataset::Pool from a Python object that contains it
 */
std::shared_ptr<arki::dataset::Pool> pool_from_python(PyObject* o);

void register_dataset_session(PyObject* module);

} // namespace python
} // namespace arki
#endif
