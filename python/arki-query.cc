#include "arki-query.h"
#include "arki/dataset.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/pool.h"
#include "arki/nag.h"
#include "cmdline.h"
#include "cmdline/processor.h"
#include "common.h"
#include "dataset/session.h"
#include "utils/core.h"
#include "utils/methods.h"
#include <sysexits.h>

using namespace arki::python;
using namespace arki::utils;

extern "C" {

PyTypeObject* arkipy_ArkiQuery_Type = nullptr;
}

namespace {

struct set_processor : public MethKwargs<set_processor, arkipy_ArkiQuery>
{
    constexpr static const char* name      = "set_processor";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "";
    constexpr static const char* summary   = "set dataset processor";
    constexpr static const char* doc       = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try
        {
            auto processor  = build_processor(self->pool, args, kw);
            self->processor = processor.release();
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct query_file : public MethKwargs<query_file, arkipy_ArkiQuery>
{
    constexpr static const char* name      = "query_file";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "int";
    constexpr static const char* summary   = "run arki-query --stdin";
    constexpr static const char* doc       = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"file", "format", nullptr};

        PyObject* file   = nullptr;
        PyObject* format = nullptr;
        if (!PyArg_ParseTupleAndKeywords(
                args, kw, "OO", const_cast<char**>(kwlist), &file, &format))
            return nullptr;

        try
        {
            auto dest = [&](arki::dataset::Reader& reader) {
                self->processor->process(reader, reader.name());
            };

            bool all_successful = true;
            {
                BinaryInputFile in(file);
                ReleaseGIL rg;
                all_successful =
                    foreach_file(self->pool->session(), in,
                                 dataformat_from_python(format), dest);
                self->processor->end();
            }

            if (all_successful)
                return throw_ifnull(PyLong_FromLong(EX_OK));
            else
                return throw_ifnull(PyLong_FromLong(EX_SOFTWARE));
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct query_merged : public MethKwargs<query_merged, arkipy_ArkiQuery>
{
    constexpr static const char* name      = "query_merged";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "int";
    constexpr static const char* summary   = "run arki-query --merged";
    constexpr static const char* doc       = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {nullptr};

        if (!PyArg_ParseTupleAndKeywords(args, kw, "",
                                         const_cast<char**>(kwlist)))
            return nullptr;

        try
        {
            bool all_successful = true;
            {
                ReleaseGIL rg;

                auto dataset = std::make_shared<arki::dataset::merged::Dataset>(
                    self->pool);
                auto reader = dataset->create_reader();

                try
                {
                    self->processor->process(*reader, dataset->name());
                }
                catch (PythonException& e)
                {
                    throw;
                }
                catch (std::exception& e)
                {
                    arki::nag::warning("%s failed: %s", dataset->name().c_str(),
                                       e.what());
                    all_successful = false;
                }

                self->processor->end();
            }

            if (all_successful)
                return throw_ifnull(PyLong_FromLong(EX_OK));
            else
                return throw_ifnull(PyLong_FromLong(EX_SOFTWARE));
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct query_qmacro : public MethKwargs<query_qmacro, arkipy_ArkiQuery>
{
    constexpr static const char* name      = "query_qmacro";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "int";
    constexpr static const char* summary   = "run arki-query --qmacro";
    constexpr static const char* doc       = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"macro_name", "macro_query", nullptr};
        const char* arg_macro_name  = nullptr;
        Py_ssize_t arg_macro_name_len;
        const char* arg_macro_query = nullptr;
        Py_ssize_t arg_macro_query_len;
        if (!PyArg_ParseTupleAndKeywords(
                args, kw, "z#z#", const_cast<char**>(kwlist), &arg_macro_name,
                &arg_macro_name_len, &arg_macro_query, &arg_macro_query_len))
            return nullptr;

        try
        {
            bool all_successful = true;
            {
                ReleaseGIL rg;

                std::string macro_name(arg_macro_name, arg_macro_name_len);
                std::string macro_query(arg_macro_query, arg_macro_query_len);

                auto dataset = self->pool->querymacro(macro_name, macro_query);
                auto reader  = dataset->create_reader();

                try
                {
                    self->processor->process(*reader, reader->name());
                }
                catch (PythonException&)
                {
                    throw;
                }
                catch (std::exception& e)
                {
                    arki::nag::warning("%s failed: %s", reader->name().c_str(),
                                       e.what());
                    all_successful = false;
                }

                self->processor->end();
            }

            if (all_successful)
                return throw_ifnull(PyLong_FromLong(EX_OK));
            else
                return throw_ifnull(PyLong_FromLong(EX_SOFTWARE));
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct query_sections : public MethKwargs<query_sections, arkipy_ArkiQuery>
{
    constexpr static const char* name      = "query_sections";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "int";
    constexpr static const char* summary   = "run arki-query";
    constexpr static const char* doc       = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {nullptr};
        if (!PyArg_ParseTupleAndKeywords(args, kw, "",
                                         const_cast<char**>(kwlist)))
            return nullptr;

        try
        {
            auto dest = [&](arki::dataset::Reader& reader) {
                self->processor->process(reader, reader.name());
            };

            bool all_successful = true;
            {
                ReleaseGIL rg;
                all_successful = foreach_sections(self->pool, dest);
                self->processor->end();
            }

            if (all_successful)
                return throw_ifnull(PyLong_FromLong(EX_OK));
            else
                return throw_ifnull(PyLong_FromLong(EX_SOFTWARE));
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct ArkiQueryDef : public Type<ArkiQueryDef, arkipy_ArkiQuery>
{
    constexpr static const char* name      = "ArkiQuery";
    constexpr static const char* qual_name = "arkimet.ArkiQuery";
    constexpr static const char* doc       = R"(
arki-query implementation
)";
    GetSetters<> getsetters;
    Methods<set_processor, query_file, query_merged, query_qmacro,
            query_sections>
        methods;

    static void _dealloc(Impl* self)
    {
        self->pool.~shared_ptr<arki::dataset::Pool>();
        delete self->processor;
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self) { return PyUnicode_FromString(name); }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += " object";
        return PyUnicode_FromString(res.c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[]    = {"session", nullptr};
        arkipy_DatasetSession* session = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!",
                                         const_cast<char**>(kwlist),
                                         arkipy_DatasetSession_Type, &session))
            return -1;

        try
        {
            new (&(self->pool))
                std::shared_ptr<arki::dataset::Pool>(session->pool);
            self->processor = nullptr;
        }
        ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiQueryDef* arki_query_def = nullptr;

} // namespace

namespace arki {
namespace python {

void register_arki_query(PyObject* m)
{
    arki_query_def = new ArkiQueryDef;
    arki_query_def->define(arkipy_ArkiQuery_Type, m);
}

} // namespace python
} // namespace arki
