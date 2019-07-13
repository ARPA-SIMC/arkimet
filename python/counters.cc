#include "counters.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "arki/utils/accounting.h"
#include "common.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_countersCounter_Type = nullptr;

}


namespace {

#if 0
struct expanded : public Getter<expanded, arkipy_Matcher>
{
    constexpr static const char* name = "expanded";
    constexpr static const char* doc = "return the query with all aliases expanded";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try {
            return to_python(self->matcher->toStringExpanded());
        } ARKI_CATCH_RETURN_PYO;
    }
};

struct match : public MethKwargs<match, arkipy_Matcher>
{
    constexpr static const char* name = "match";
    constexpr static const char* signature = "?";
    constexpr static const char* returns = "bool";
    constexpr static const char* summary = "return the result of trying to match the given ?";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try {
        } ARKI_CATCH_RETURN_PYO
    }
};
#endif

struct CounterDef : public Type<CounterDef, arkipy_countersCounter>
{
    constexpr static const char* name = "Counter";
    constexpr static const char* qual_name = "arkimet.counters.Counter";
    constexpr static const char* doc = R"(
Counter used to debug arkimet I/O operations
)";
    GetSetters<> getsetters;
    Methods<> methods;

    static void _dealloc(Impl* self)
    {
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        try {
            std::string str = self->counter->name();
            str += ":";
            str += std::to_string(self->counter->val());
            pyo_unique_ptr res = to_python(str);
            return res.release();
        } ARKI_CATCH_RETURN_PYO;
    }

    static PyObject* _repr(Impl* self)
    {
        try {
            std::string str = "Counter(";
            str += self->counter->name();
            str += ":";
            str += std::to_string(self->counter->val());
            str += ")";
            pyo_unique_ptr res = to_python(str);
            return res.release();
        } ARKI_CATCH_RETURN_PYO;
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        PyErr_SetString(PyExc_NotImplementedError, "Counter objects cannot be constructed explicitly");
        return -1;
    }
};

CounterDef* counter_def = nullptr;

PyObject* make_counter(arki::utils::acct::Counter& counter)
{
    py_unique_ptr<arkipy_countersCounter> res(throw_ifnull(PyObject_New(arkipy_countersCounter, arkipy_countersCounter_Type)));
    res->counter = &counter;
    return (PyObject*)res.release();
}

Methods<> counters_methods;

}

extern "C" {

static PyModuleDef counters_module = {
    PyModuleDef_HEAD_INIT,
    "counters",         /* m_name */
    "Arkimet performance counters",  /* m_doc */
    -1,             /* m_size */
    counters_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

}

namespace arki {
namespace python {

void register_counters(PyObject* m)
{
    counter_def = new CounterDef;
    counter_def->define(arkipy_countersCounter_Type);

    pyo_unique_ptr counters = throw_ifnull(PyModule_Create(&counters_module));

    pyo_unique_ptr c(make_counter(arki::utils::acct::plain_data_read_count));
    if (PyModule_AddObject(counters, "plain_data_read_count", c.release()) == -1)
        throw PythonException();

    c.reset(make_counter(arki::utils::acct::gzip_data_read_count));
    if (PyModule_AddObject(counters, "gzip_data_read_count", c.release()) == -1)
        throw PythonException();

    c.reset(make_counter(arki::utils::acct::gzip_forward_seek_bytes));
    if (PyModule_AddObject(counters, "gzip_forward_seek_bytes", c.release()) == -1)
        throw PythonException();

    c.reset(make_counter(arki::utils::acct::gzip_idx_reposition_count));
    if (PyModule_AddObject(counters, "gzip_idx_reposition_count", c.release()) == -1)
        throw PythonException();

    c.reset(make_counter(arki::utils::acct::acquire_single_count));
    if (PyModule_AddObject(counters, "acquire_single_count", c.release()) == -1)
        throw PythonException();

    c.reset(make_counter(arki::utils::acct::acquire_batch_count));
    if (PyModule_AddObject(counters, "acquire_batch_count", c.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(m, "counters", counters.release()) == -1)
        throw PythonException();
}

}
}

