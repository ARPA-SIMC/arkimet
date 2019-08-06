#ifndef ARKI_PYTHON_EMITTER_H
#define ARKI_PYTHON_EMITTER_H

#include <arki/emitter.h>
#include "utils/core.h"

namespace arki {
namespace python {

struct PythonEmitter : public Emitter
{
    struct Target
    {
        enum State {
            LIST,
            MAPPING,
            MAPPING_KEY,
        } state;
        PyObject* o = nullptr;

        Target(State state, PyObject* o) : state(state), o(o) {}
    };
    std::vector<Target> stack;
    PyObject* res = nullptr;

    ~PythonEmitter();

    PyObject* release()
    {
        PyObject* o = res;
        res = nullptr;
        return o;
    }

    /**
     * Adds a value to the python object that is currnetly been built.
     *
     * Steals a reference to o.
     */
    void add_object(PyObject* o);

    void start_list() override;
    void end_list() override;

    void start_mapping() override;
    void end_mapping() override;

    void add_null() override;
    void add_bool(bool val) override;
    void add_int(long long int val) override;
    void add_double(double val) override;
    void add_string(const std::string& val) override;
};


}
}
#endif
