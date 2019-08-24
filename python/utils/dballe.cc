#include "dballe.h"

namespace arki {
namespace python {

Dballe::Dballe()
{
    // We can't throw here, because we should support the object being
    // instantiated at module level
}

Dballe::~Dballe()
{
}

void Dballe::import()
{
    // Make sure we are idempotent on double import
    if (m_api)
        return;

    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("dballe")));

    m_api = (dbapy_c_api*)PyCapsule_Import("_dballe._C_API", 0);
    if (!m_api)
        throw PythonException();

#ifdef DBALLE_API1_MIN_VERSION
    if (m_api->version_major != 1)
    {
        PyErr_Format(PyExc_RuntimeError, "dballe C API version is %d.%d but only 1.x is supported",
                     m_api->version_major, m_api->version_minor);
        throw PythonException();
    }

    if (m_api->version_minor < DBALLE_API1_MIN_VERSION)
    {
        PyErr_Format(PyExc_RuntimeError, "dballe C API version is %d.%d but only 1.x is supported, with x > %d",
                     m_api->version_major, m_api->version_minor, DBALLE_API1_MIN_VERSION);
        throw PythonException();
    }
#endif
}

void Dballe::require_imported() const
{
    if (!m_api)
    {
        PyErr_SetString(PyExc_RuntimeError, "attempted to use the dballe C API without importing it");
        throw PythonException();
    }
}

}
}
