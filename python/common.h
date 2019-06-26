#ifndef ARKI_PYTHON_COMMON_H
#define ARKI_PYTHON_COMMON_H

#include "utils/core.h"
#include <string>
#include <vector>
#include <arki/emitter.h>

namespace arki {
struct ConfigFile;

namespace python {

struct python_callback_failed : public std::exception {};


/// Given a generic exception, set the Python error indicator appropriately.
void set_std_exception(const std::exception& e);


#define ARKI_CATCH_RETURN_PYO \
      catch (python_callback_failed&) { \
        return nullptr; \
    } catch (std::exception& se) { \
        set_std_exception(se); return nullptr; \
    }

#define ARKI_CATCH_RETURN_INT \
      catch (python_callback_failed&) { \
        return -1; \
    } catch (std::exception& se) { \
        set_std_exception(se); return -1; \
    }

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
 * If o is a Long, return its value. Else call o.fileno() and return its
 * result.
 *
 * Returns -1 if fileno() was not available or an error occurred.
 */
int file_get_fileno(PyObject* o);

/**
 * Convert a python string, bytes or unicode to an utf8 string.
 *
 * Return 0 on success, -1 on failure
 */
int string_from_python(PyObject* o, std::string& out);

/**
 * Fill a ConfigFile with configuration info from python.
 *
 * Currently this only supports:
 *  - str or bytes, that will get parsed by ConfigFile.
 *  - dict, that will be set as key->val into out
 *
 * In the future it can support reading data from a ConfigFile object.
 */
int configfile_from_python(PyObject* o, ConfigFile& out);

/**
 * Initialize the python bits to use used by the common functions.
 *
 * This can be called multiple times and will execute only once.
 */
int common_init();

}
}
#endif
