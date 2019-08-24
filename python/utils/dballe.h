#ifndef ARKI_PYTHON_DBALLE_H
#define ARKI_PYTHON_DBALLE_H

#include <dballe/python.h>
#include "core.h"

#define DBALLE_API1_MIN_VERSION 0

namespace arki {
namespace python {

class Dballe
{
protected:
    dbapy_c_api* m_api = nullptr;

public:
    Dballe();
    ~Dballe();

    void import();

    /// Throw a python error if import() was not called
    void require_imported() const;

    dbapy_c_api& api() { require_imported(); return *m_api; }
    const dbapy_c_api& api() const { require_imported(); return *m_api; }

    /// Check if an object is a wreport.Var
    bool message_check(PyObject* ob) const
    {
        auto& dba = api();
        return Py_TYPE(ob) == dba.message_type ||
               PyType_IsSubtype(Py_TYPE(ob), dba.message_type);
    }

    /// Create a new unset wreport.Var object
    PyObject* message_create(dballe::MessageType type) const
    {
        return (PyObject*)throw_ifnull(api().message_create_new(type));
    }

    /// Create a new wreport.Var object with an integer value
    PyObject* message_create(std::shared_ptr<dballe::Message> msg) const
    {
        return (PyObject*)throw_ifnull(api().message_create(msg));
    }

#if DBALLE_API1_MIN_VERSION >= 1
#endif
};

}
}

#endif
