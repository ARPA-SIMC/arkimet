#include "arki-xargs.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/metadata/xargs.h"
#include "arki/utils/sys.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"

using namespace arki::utils;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_ArkiXargs_Type = nullptr;

}


namespace {

struct run_ : public MethKwargs<run_, arkipy_ArkiXargs>
{
    constexpr static const char* name = "run";
    constexpr static const char* signature = "command: Sequence[str], inputs: Sequence[str]=None, max_args: int=None, max_size: str=None, time_interval: str=None, split_timerange: bool=False";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-xargs";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "command", "inputs", "max_args", "max_size", "time_interval", "split_timerange", nullptr };
        PyObject* py_command = nullptr;
        PyObject* py_inputs = nullptr;
        PyObject* py_max_args = 0;
        unsigned long long max_size = 0;
        const char* time_interval = nullptr;
        Py_ssize_t time_interval_len;
        int split_timerange = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|OOKz#p", const_cast<char**>(kwlist),
                    &py_command, &py_inputs, &py_max_args, &max_size,
                    &time_interval, &time_interval_len, &split_timerange))
            return nullptr;

        try {
            arki::metadata::Xargs consumer;
            consumer.command = stringlist_from_python(py_command);
            if (py_max_args && py_max_args != Py_None)
                consumer.max_count = int_from_python(py_max_args);
            if (max_size)
                consumer.set_max_bytes(max_size);
            if (time_interval)
                consumer.set_interval(std::string(time_interval, time_interval_len));
            if (split_timerange)
                consumer.split_timerange = true;

            if (!py_inputs || py_inputs != Py_None)
            {
                auto inputs = stringlist_from_python(py_inputs);
                ReleaseGIL rg;
                // Process the files
                for (const auto& i: inputs)
                {
                    sys::File in(i, O_RDONLY);
                    arki::metadata::ReadContext rc(sys::getcwd(), in.name());
                    arki::Metadata::read_file(in, rc, [&](std::shared_ptr<arki::Metadata> md) { return consumer.eat(md); });
                }
                consumer.flush();
            } else {
                ReleaseGIL rg;
                // Process stdin
                arki::core::Stdin in;
                arki::metadata::ReadContext rc(sys::getcwd(), in.name());
                arki::Metadata::read_file(in, rc, [&](std::shared_ptr<arki::Metadata> md) { return consumer.eat(md); });
                consumer.flush();
            }

            return throw_ifnull(PyLong_FromLong(0));
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiXargsDef : public Type<ArkiXargsDef, arkipy_ArkiXargs>
{
    constexpr static const char* name = "ArkiXargs";
    constexpr static const char* qual_name = "arkimet.ArkiXargs";
    constexpr static const char* doc = R"(
arki-xargs implementation
)";
    GetSetters<> getsetters;
    Methods<run_> methods;

    static void _dealloc(Impl* self)
    {
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString(name);
    }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += " object";
        return PyUnicode_FromString(res.c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiXargsDef* arki_xargs_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_xargs(PyObject* m)
{
    arki_xargs_def = new ArkiXargsDef;
    arki_xargs_def->define(arkipy_ArkiXargs_Type, m);

}

}
}
