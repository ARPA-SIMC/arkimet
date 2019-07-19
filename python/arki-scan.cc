#include "arki/runtime/arki-scan.h"
#include "arki-scan.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include "cmdline.h"

using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_ArkiScan_Type = nullptr;

}


namespace {

struct set_inputs : public MethKwargs<set_inputs, arkipy_ArkiScan>
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
            self->arki_scan->inputs = sections_from_python(py_config);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct set_processor : public MethKwargs<set_processor, arkipy_ArkiScan>
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
            self->arki_scan->processor = processor.release();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct set_dispatcher : public MethKwargs<set_dispatcher, arkipy_ArkiScan>
{
    constexpr static const char* name = "set_dispatcher";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "set dispatcher";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try {
            auto dispatcher = build_dispatcher(*(self->arki_scan->processor), args, kw);
            self->arki_scan->dispatcher = dispatcher.release();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct scan_stdin : public MethKwargs<scan_stdin, arkipy_ArkiScan>
{
    constexpr static const char* name = "scan_stdin";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-scan --stdin";
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
            bool all_successful = self->arki_scan->run_scan_stdin(std::string(format, format_len));
            self->arki_scan->processor->end();
            if (all_successful)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct scan_sections : public MethKwargs<scan_sections, arkipy_ArkiScan>
{
    constexpr static const char* name = "scan_sections";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-scan";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "moveok", "moveko", "movework", nullptr };
        const char* moveok = nullptr;
        Py_ssize_t moveok_len;
        const char* moveko = nullptr;
        Py_ssize_t moveko_len;
        const char* movework = nullptr;
        Py_ssize_t movework_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#z#z#", const_cast<char**>(kwlist),
                    &moveok, &moveok_len,
                    &moveko, &moveko_len,
                    &movework, &movework_len))
            return nullptr;

        try {
            bool all_successful = self->arki_scan->run_scan_inputs(
                    moveok ? std::string(moveok, moveok_len) : std::string(),
                    moveko ? std::string(moveko, moveko_len) : std::string(),
                    movework ? std::string(movework, movework_len) : std::string());
            self->arki_scan->processor->end();
            if (all_successful)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiScanDef : public Type<ArkiScanDef, arkipy_ArkiScan>
{
    constexpr static const char* name = "ArkiScan";
    constexpr static const char* qual_name = "arkimet.ArkiScan";
    constexpr static const char* doc = R"(
arki-scan implementation
)";
    GetSetters<> getsetters;
    Methods<set_inputs, set_processor, set_dispatcher, scan_stdin, scan_sections> methods;

    static void _dealloc(Impl* self)
    {
        delete self->arki_scan;
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
            self->arki_scan = new arki::runtime::ArkiScan;
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiScanDef* arki_scan_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_scan(PyObject* m)
{
    arki_scan_def = new ArkiScanDef;
    arki_scan_def->define(arkipy_ArkiScan_Type, m);

}

}
}
