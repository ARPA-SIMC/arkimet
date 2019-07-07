#include "arki/runtime/arki-bufr-prepare.h"
#include "arki-bufr-prepare.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_ArkiBUFRPrepare_Type = nullptr;

}


namespace {

struct run_ : public MethKwargs<run_, arkipy_ArkiBUFRPrepare>
{
    constexpr static const char* name = "run";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-bufr-prepare";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "inputs", "usn", "outfile", "failfile", nullptr };
        PyObject* arg_inputs = nullptr;
        PyObject* arg_usn = nullptr;
        const char* arg_outfile = nullptr;
        Py_ssize_t arg_outfile_len;
        const char* arg_failfile = nullptr;
        Py_ssize_t arg_failfile_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|OOz#z#", const_cast<char**>(kwlist),
                    &arg_inputs, &arg_usn,
                    &arg_outfile, &arg_outfile_len,
                    &arg_failfile, &arg_failfile_len))
            return nullptr;

        try {
            if (arg_inputs)
                self->arki_bufr_prepare->inputs = stringlist_from_python(arg_inputs);

            if (arg_outfile)
                self->arki_bufr_prepare->outfile = std::string(arg_outfile, arg_outfile_len);
            if (arg_failfile)
                self->arki_bufr_prepare->failfile = std::string(arg_failfile, arg_failfile_len);
            if (arg_usn && arg_usn != Py_None)
            {
                self->arki_bufr_prepare->force_usn = true;
                self->arki_bufr_prepare->forced_usn = from_python<int>(arg_usn);
            }

            int res;
            {
                ReleaseGIL rg;
                res = self->arki_bufr_prepare->run();
            }
            return throw_ifnull(PyLong_FromLong(res));
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiBUFRPrepareDef : public Type<ArkiBUFRPrepareDef, arkipy_ArkiBUFRPrepare>
{
    constexpr static const char* name = "ArkiBUFRPrepare";
    constexpr static const char* qual_name = "arkimet.ArkiBUFRPrepare";
    constexpr static const char* doc = R"(
arki-bufr-prepare implementation
)";
    GetSetters<> getsetters;
    Methods<run_> methods;

    static void _dealloc(Impl* self)
    {
        delete self->arki_bufr_prepare;
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
            self->arki_bufr_prepare = new arki::runtime::ArkiBUFRPrepare;
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiBUFRPrepareDef* arki_bufr_prepare_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_bufr_prepare(PyObject* m)
{
    arki_bufr_prepare_def = new ArkiBUFRPrepareDef;
    arki_bufr_prepare_def->define(arkipy_ArkiBUFRPrepare_Type, m);

}

}
}
