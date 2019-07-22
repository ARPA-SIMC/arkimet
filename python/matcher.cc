#include "matcher.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_Matcher_Type = nullptr;

}


namespace {

struct expanded : public Getter<expanded, arkipy_Matcher>
{
    constexpr static const char* name = "expanded";
    constexpr static const char* doc = "return the query with all aliases expanded";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try {
            return to_python(self->matcher.toStringExpanded());
        } ARKI_CATCH_RETURN_PYO;
    }
};

#if 0
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

struct MatcherDef : public Type<MatcherDef, arkipy_Matcher>
{
    constexpr static const char* name = "Matcher";
    constexpr static const char* qual_name = "arkimet.Matcher";
    constexpr static const char* doc = R"(
Precompiled matcher for arkimet metadata
)";
    GetSetters<expanded> getsetters;
    Methods<> methods;

    static void _dealloc(Impl* self)
    {
        self->matcher.~Matcher();
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        try {
            pyo_unique_ptr res = to_python(self->matcher.toString());
            return res.release();
        } ARKI_CATCH_RETURN_PYO;
    }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += "(" + self->matcher.toString() + ")";
        return PyUnicode_FromString(res.c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "expr", nullptr };
        const char* arg_expr = nullptr;
        Py_ssize_t arg_expr_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|s#", const_cast<char**>(kwlist), &arg_expr, &arg_expr_len))
            return -1;

        try {
            if (arg_expr)
                new(&(self->matcher)) arki::Matcher(arki::Matcher::parse(std::string(arg_expr, arg_expr_len)));
            else
                new(&(self->matcher)) arki::Matcher();
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

MatcherDef* matcher_def = nullptr;

}

namespace arki {
namespace python {

PyObject* matcher(arki::Matcher matcher)
{
    py_unique_ptr<arkipy_Matcher> res(throw_ifnull(PyObject_New(arkipy_Matcher, arkipy_Matcher_Type)));
    new (&(res->matcher)) arki::Matcher(matcher);
    return (PyObject*)res.release();
}

void register_matcher(PyObject* m)
{
    matcher_def = new MatcherDef;
    matcher_def->define(arkipy_Matcher_Type, m);
}

}
}
