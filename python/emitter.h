#ifndef ARKI_PYTHON_EMITTER_H
#define ARKI_PYTHON_EMITTER_H

#include <arki/structured/emitter.h>
#include <arki/structured/reader.h>
#include "utils/core.h"

namespace arki {
namespace python {

struct PythonEmitter : public structured::Emitter
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


struct PythonReader : public structured::Reader
{
protected:
    PyObject* o;

public:
    PythonReader(PyObject* o)
        : o(o)
    {
        Py_INCREF(o);
    }
    PythonReader(const PythonReader& o)
        : o (o.o)
    {
        Py_INCREF(this->o);
    }
    PythonReader(PythonReader&& o)
        : o (o.o)
    {
        Py_INCREF(this->o);
    }
    PythonReader& operator=(const PythonReader& o)
    {
        if (this->o == o.o)
            return *this;
        Py_DECREF(this->o);
        this->o = o.o;
        Py_INCREF(this->o);
        return *this;
    }
    PythonReader& operator=(PythonReader&& o)
    {
        if (this->o == o.o)
            return *this;
        Py_DECREF(this->o);
        this->o = o.o;
        Py_INCREF(this->o);
        return *this;
    }
    ~PythonReader()
    {
        Py_DECREF(o);
    }
    structured::NodeType type() const override;
    std::string repr() const override;

    bool as_bool(const char* desc) const override;
    long long int as_int(const char* desc) const override;
    double as_double(const char* desc) const override;
    std::string as_string(const char* desc) const override;

    unsigned list_size(const char* desc) const override;
    bool as_bool(unsigned idx, const char* desc) const override;
    long long int as_int(unsigned idx, const char* desc) const override;
    double as_double(unsigned idx, const char* desc) const override;
    std::string as_string(unsigned idx, const char* desc) const override;
    void sub(unsigned idx, const char* desc, std::function<void(const Reader&)>) const override;

    bool has_key(const std::string& key, structured::NodeType type) const override;
    bool as_bool(const std::string& key, const char* desc) const override;
    long long int as_int(const std::string& key, const char* desc) const override;
    double as_double(const std::string& key, const char* desc) const override;
    std::string as_string(const std::string& key, const char* desc) const override;
    core::Time as_time(const std::string& key, const char* desc) const override;
    void items(const char* desc, std::function<void(const std::string&, const Reader&)>) const override;
    void sub(const std::string& key, const char* desc, std::function<void(const Reader&)>) const override;
};


}
}
#endif
