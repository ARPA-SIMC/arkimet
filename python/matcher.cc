#include "matcher.h"
#include "arki/dataset/session.h"
#include "arki/metadata.h"
#include "common.h"
#include "dataset.h"
#include "metadata.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_Matcher_Type = nullptr;
}

namespace {

struct expanded : public Getter<expanded, arkipy_Matcher>
{
    constexpr static const char* name = "expanded";
    constexpr static const char* doc =
        "return the query with all aliases expanded";
    constexpr static void* closure = nullptr;

    static PyObject* get(Impl* self, void* closure)
    {
        try
        {
            return to_python(self->matcher.toStringExpanded());
        }
        ARKI_CATCH_RETURN_PYO;
    }
};

struct match : public MethKwargs<match, arkipy_Matcher>
{
    constexpr static const char* name      = "match";
    constexpr static const char* signature = "md: arki.Metadata";
    constexpr static const char* returns   = "bool";
    constexpr static const char* summary =
        "return the result of trying to match the given metadata";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"md", nullptr};
        PyObject* py_md             = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!",
                                         const_cast<char**>(kwlist),
                                         arkipy_Metadata_Type, &py_md))
            return nullptr;

        try
        {
            bool res = self->matcher(*((arkipy_Metadata*)py_md)->md);
            if (res)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct merge : public MethKwargs<merge, arkipy_Matcher>
{
    constexpr static const char* name      = "merge";
    constexpr static const char* signature = "matcher: arki.Matcher";
    constexpr static const char* returns   = "arki.Matcher";
    constexpr static const char* summary =
        "return a matcher that selects at least the data from this and the "
        "other matcher";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"matcher", nullptr};
        PyObject* py_matcher        = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!",
                                         const_cast<char**>(kwlist),
                                         arkipy_Matcher_Type, &py_matcher))
            return nullptr;

        try
        {
            auto res =
                self->matcher.merge(((arkipy_Matcher*)py_matcher)->matcher);
            return matcher_to_python(res);
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct update : public MethKwargs<update, arkipy_Matcher>
{
    constexpr static const char* name      = "update";
    constexpr static const char* signature = "matcher: arki.Matcher";
    constexpr static const char* returns   = "arki.Matcher";
    constexpr static const char* summary =
        "return this matcher plus all the rules of the other matcher. When "
        "they both have a rule for the same metadata, only the rule of the "
        "second matcher is kept";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"matcher", nullptr};
        PyObject* py_matcher        = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!",
                                         const_cast<char**>(kwlist),
                                         arkipy_Matcher_Type, &py_matcher))
            return nullptr;

        try
        {
            auto res =
                self->matcher.update(((arkipy_Matcher*)py_matcher)->matcher);
            return matcher_to_python(res);
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct MatcherDef : public Type<MatcherDef, arkipy_Matcher>
{
    constexpr static const char* name      = "Matcher";
    constexpr static const char* qual_name = "arkimet.Matcher";
    constexpr static const char* doc       = R"(
Precompiled matcher for arkimet metadata
)";
    GetSetters<expanded> getsetters;
    Methods<match, merge, update> methods;

    static void _dealloc(Impl* self)
    {
        self->matcher.~Matcher();
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        try
        {
            pyo_unique_ptr res = to_python(self->matcher.toString());
            return res.release();
        }
        ARKI_CATCH_RETURN_PYO;
    }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += "(" + self->matcher.toString() + ")";
        return PyUnicode_FromString(res.c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"expr", nullptr};
        PyObject* arg_matcher       = nullptr;
        if (!PyArg_ParseTupleAndKeywords(
                args, kw, "|O", const_cast<char**>(kwlist), &arg_matcher))
            return -1;

        try
        {
            if (arg_matcher)
            {
                if (arkipy_Matcher_Check(arg_matcher))
                    new (&(self->matcher))
                        arki::Matcher(((arkipy_Matcher*)arg_matcher)->matcher);
                else
                {
                    if (PyErr_WarnEx(PyExc_DeprecationWarning,
                                     "Use arki.dataset.Session().matcher(str) "
                                     "instead of arkimet.Matcher(str)",
                                     1))
                        return -1;

                    auto session = std::make_shared<arki::dataset::Session>();
                    new (&(self->matcher)) arki::Matcher(session->matcher(
                        from_python<std::string>(arg_matcher)));
                }
            }
            else
                new (&(self->matcher)) arki::Matcher();
        }
        ARKI_CATCH_RETURN_INT

        return 0;
    }
};

MatcherDef* matcher_def = nullptr;

} // namespace

namespace arki {
namespace python {

PyObject* matcher_to_python(arki::Matcher matcher)
{
    py_unique_ptr<arkipy_Matcher> res(
        throw_ifnull(PyObject_New(arkipy_Matcher, arkipy_Matcher_Type)));
    new (&(res->matcher)) arki::Matcher(matcher);
    return (PyObject*)res.release();
}

arki::Matcher
matcher_from_python(std::shared_ptr<arki::dataset::Session> session,
                    PyObject* o)
{
    if (o == Py_None)
        return arki::Matcher();

    if (arkipy_Matcher_Check(o))
        return ((arkipy_Matcher*)o)->matcher;

    return session->matcher(from_python<std::string>(o));
}

void register_matcher(PyObject* m)
{
    matcher_def = new MatcherDef;
    matcher_def->define(arkipy_Matcher_Type, m);
}

} // namespace python
} // namespace arki
