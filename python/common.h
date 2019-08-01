#ifndef ARKI_PYTHON_COMMON_H
#define ARKI_PYTHON_COMMON_H

#include "utils/core.h"
#include "utils/values.h"
#include <string>
#include <vector>
#include <memory>
#include <arki/defs.h>
#include <arki/emitter.h>
#include <arki/core/fwd.h>

namespace arki {
struct Matcher;

namespace python {

/// Given a generic exception, set the Python error indicator appropriately.
void set_std_exception(const std::exception& e);


#define ARKI_CATCH_RETURN_PYO \
      catch (PythonException&) { \
        return nullptr; \
    } catch (std::invalid_argument& e) { \
        PyErr_SetString(PyExc_ValueError, e.what()); return nullptr; \
    } catch (std::exception& se) { \
        set_std_exception(se); return nullptr; \
    }

#define ARKI_CATCH_RETURN_INT \
      catch (PythonException&) { \
        return -1; \
    } catch (std::invalid_argument& e) { \
        PyErr_SetString(PyExc_ValueError, e.what()); return -1; \
    } catch (std::exception& se) { \
        set_std_exception(se); return -1; \
    }

#define ARKI_CATCH_RETHROW_PYTHON \
      catch (PythonException&) { \
        throw; \
    } catch (std::invalid_argument& e) { \
        PyErr_SetString(PyExc_ValueError, e.what()); throw PythonException(); \
    } catch (std::exception& se) { \
        set_std_exception(se); throw PythonException(); \
    }


// Define Py_RETURN_RICHCOMPARE for compatibility for pre-3.7 python versions
#ifndef Py_RETURN_RICHCOMPARE
#define Py_RETURN_RICHCOMPARE(val1, val2, op)                               \
    do {                                                                    \
        switch (op) {                                                       \
        case Py_EQ: if ((val1) == (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_NE: if ((val1) != (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_LT: if ((val1) < (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;   \
        case Py_GT: if ((val1) > (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;   \
        case Py_LE: if ((val1) <= (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        case Py_GE: if ((val1) >= (val2)) Py_RETURN_TRUE; Py_RETURN_FALSE;  \
        default:                                                            \
            abort();                                                        \
        }                                                                   \
    } while (0)
#endif

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

/// Call repr() on \a o, and return the result in \a out
int object_repr(PyObject* o, std::string& out);

/**
 * Create a cfg::Section from python.
 *
 * Currently this only supports:
 *  - str or bytes, that will get parsed
 *  - dict, that will be set as key->val into out
 */
core::cfg::Section section_from_python(PyObject* o);

/**
 * Create a cfg::Sections from python.
 *
 * Currently this only supports:
 *  - str or bytes, that will get parsed
 */
core::cfg::Sections sections_from_python(PyObject* o);

/**
 * Create a LineReader to read from any python object that can iterate strings
 */
std::unique_ptr<core::LineReader> linereader_from_python(PyObject* o);

/**
 * Initialize the python bits to use used by the common functions.
 *
 * This can be called multiple times and will execute only once.
 */
int common_init();

}
}
#endif
