#include "arki/runtime/arki-query.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/source.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/matcher.h"
#include "arki-query.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include "cmdline.h"

using namespace arki::python;
using namespace arki::utils;

extern "C" {

PyTypeObject* arkipy_ArkiQuery_Type = nullptr;

}


namespace {

struct set_inputs : public MethKwargs<set_inputs, arkipy_ArkiQuery>
{
    constexpr static const char* name = "set_inputs";
    constexpr static const char* signature = "config: arkimet.cfg.Sections";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "set input configuration";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "config", nullptr };
        PyObject* py_config = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_config))
            return nullptr;

        try {
            self->arki_query->inputs = sections_from_python(py_config);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct set_processor : public MethKwargs<set_processor, arkipy_ArkiQuery>
{
    constexpr static const char* name = "set_processor";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "set dataset processor";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try {
            auto processor = build_processor(args, kw);
            self->arki_query->processor = processor.release();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_stdin : public MethKwargs<query_stdin, arkipy_ArkiQuery>
{
    constexpr static const char* name = "query_stdin";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-query --stdin";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "format", nullptr };

        const char* format = nullptr;
        Py_ssize_t format_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "z#", const_cast<char**>(kwlist),
                    &format, &format_len))
            return nullptr;

        try {
            auto dest = [&](arki::runtime::Source& source) {
                self->arki_query->processor->process(source.reader(), source.name());
                return true;
            };

            bool all_successful = true;
            {
                ReleaseGIL rg;
                all_successful = arki::runtime::foreach_stdin(
                        std::string(format, format_len), dest);
                self->arki_query->processor->end();
            }

            if (all_successful)
                return throw_ifnull(PyLong_FromLong(0));
            else
                return throw_ifnull(PyLong_FromLong(1));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_merged : public MethKwargs<query_merged, arkipy_ArkiQuery>
{
    constexpr static const char* name = "query_merged";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-query --merged";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };

        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return nullptr;

        try {
            auto dest = [&](arki::runtime::Source& source) {
                self->arki_query->processor->process(source.reader(), source.name());
                return true;
            };

            bool all_successful = true;
            {
                ReleaseGIL rg;
                all_successful = arki::runtime::foreach_merged(
                        self->arki_query->inputs, dest);
                self->arki_query->processor->end();
            }

            if (all_successful)
                return throw_ifnull(PyLong_FromLong(0));
            else
                return throw_ifnull(PyLong_FromLong(1));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_qmacro : public MethKwargs<query_qmacro, arkipy_ArkiQuery>
{
    constexpr static const char* name = "query_qmacro";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-query --qmacro";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "macro_name", "macro_query", nullptr };
        const char* macro_name = nullptr;
        Py_ssize_t macro_name_len;
        const char* macro_query = nullptr;
        Py_ssize_t macro_query_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "z#z#", const_cast<char**>(kwlist),
                    &macro_name, &macro_name_len, &macro_query, &macro_query_len))
            return nullptr;

        try {
            auto dest = [&](arki::runtime::Source& source) {
                self->arki_query->processor->process(source.reader(), source.name());
                return true;
            };

            bool all_successful = true;
            {
                ReleaseGIL rg;
                all_successful = arki::runtime::foreach_qmacro(
                        std::string(macro_name, macro_name_len),
                        std::string(macro_query, macro_query_len),
                        self->arki_query->inputs, dest);
                self->arki_query->processor->end();
            }

            if (all_successful)
                return throw_ifnull(PyLong_FromLong(0));
            else
                return throw_ifnull(PyLong_FromLong(1));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_sections : public MethKwargs<query_sections, arkipy_ArkiQuery>
{
    constexpr static const char* name = "query_sections";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-query";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return nullptr;

        try {
            auto dest = [&](arki::runtime::Source& source) {
                self->arki_query->processor->process(source.reader(), source.name());
                return true;
            };

            bool all_successful = true;
            {
                ReleaseGIL rg;
                all_successful = arki::runtime::foreach_sections(
                        self->arki_query->inputs,
                        std::string(), std::string(), std::string(),
                        dest);
                self->arki_query->processor->end();
            }

            if (all_successful)
                return throw_ifnull(PyLong_FromLong(0));
            else
                return throw_ifnull(PyLong_FromLong(1));
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiQueryDef : public Type<ArkiQueryDef, arkipy_ArkiQuery>
{
    constexpr static const char* name = "ArkiQuery";
    constexpr static const char* qual_name = "arkimet.ArkiQuery";
    constexpr static const char* doc = R"(
arki-query implementation
)";
    GetSetters<> getsetters;
    Methods<set_inputs, set_processor, query_stdin, query_merged, query_qmacro, query_sections> methods;

    static void _dealloc(Impl* self)
    {
        delete self->arki_query;
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
            self->arki_query = new arki::runtime::ArkiQuery;
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiQueryDef* arki_query_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_query(PyObject* m)
{
    arki_query_def = new ArkiQueryDef;
    arki_query_def->define(arkipy_ArkiQuery_Type, m);

}

}
}
