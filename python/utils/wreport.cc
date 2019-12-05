#include "wreport.h"

namespace arki {
namespace python {

Wreport::Wreport()
{
    // We can't throw here, because we should support the object being
    // instantiated at module level
}

Wreport::~Wreport()
{
}

void Wreport::import()
{
    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("wreport")));

    api = (wrpy_c_api*)PyCapsule_Import("_wreport._C_API", 0);
    if (!api)
        throw PythonException();

    if (api->version_major != 1)
    {
        PyErr_Format(PyExc_RuntimeError, "wreport C API version is %d.%d but only 1.x is supported",
                     api->version_major, api->version_minor);
        throw PythonException();
    }

    if (api->version_minor < 1)
    {
        PyErr_Format(PyExc_RuntimeError, "wreport C API version is %d.%d but only 1.x is supported, with x > 1",
                     api->version_major, api->version_minor);
        throw PythonException();
    }
}

void Wreport::require_imported() const
{
    if (!api)
    {
        PyErr_SetString(PyExc_RuntimeError, "attempted to use the wreport C API without importing it");
        throw PythonException();
    }
}

}
}
