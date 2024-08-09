#include "formatter.h"
#include "structured.h"
#include "arki/formatter.h"
#include "arki/types.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include "python/utils/values.h"
#include "python/utils/methods.h"
#include "python/utils/type.h"
#include "python/common.h"

using namespace arki::utils;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_Formatter_Type = nullptr;

}

namespace {

PyObject* formatter_object = nullptr;

void load_formatter_object()
{
    using namespace arki;
    auto sources = arki::Config::get().dir_formatter.list_files(".py");
    for (const auto& source: sources)
    {
        std::string basename = str::basename(source);

        // Check if the formatter module had already been imported
        std::string module_name = "arkimet.formatter." + basename.substr(0, basename.size() - 3);
        pyo_unique_ptr py_module_name(string_to_python(module_name));
        pyo_unique_ptr module(ArkiPyImport_GetModule(py_module_name));
        if (PyErr_Occurred())
            throw PythonException();

        // Import it if needed
        if (!module)
        {
            std::string source_code = utils::sys::read_file(source);
            pyo_unique_ptr code(throw_ifnull(Py_CompileStringExFlags(
                            source_code.c_str(), source.c_str(),
                            Py_file_input, nullptr, -1)));
            module = pyo_unique_ptr(throw_ifnull(PyImport_ExecCodeModule(
                            module_name.c_str(), code)));
        }
    }

    // Get arkimet.formatter.Formatter
    pyo_unique_ptr module(throw_ifnull(PyImport_ImportModule("arkimet.formatter")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Formatter")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.Formatter forever once loaded the first time
    formatter_object = obj.release();
}


struct PythonFormatter : public arki::Formatter
{
    std::string format(const arki::types::Type& v) const override
    {
        AcquireGIL gil;
        if (!formatter_object)
            load_formatter_object();

        arki::python::PythonEmitter e;
        v.serialise(e, arki::structured::keys_python);

        pyo_unique_ptr obj(PyObject_CallMethod(
                        formatter_object, "format", "O", e.res.get()));

        if (!obj)
        {
            PyObject *type, *value, *traceback;
            PyErr_Fetch(&type, &value, &traceback);
            pyo_unique_ptr exc_type(type);
            pyo_unique_ptr exc_value(value);
            pyo_unique_ptr exc_traceback(traceback);

            arki::nag::warning("python formatter failed: %s", exc_type.str().c_str());
            return from_python<std::string>(obj);
        }

        if (obj == Py_None)
            return v.to_string();

        return from_python<std::string>(obj);
    }
};


struct format : public MethKwargs<format, arkipy_Formatter>
{
    constexpr static const char* name = "format";
    constexpr static const char* signature = "type: Dict[str, Any]";
    constexpr static const char* returns = "str";
    constexpr static const char* summary = "format the given type to a human understandable string";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "type", nullptr };
        PyObject* py_type = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_type))
            return nullptr;

        try {
            PythonReader reader(py_type);
            std::unique_ptr<arki::types::Type> type = arki::types::decode_structure(arki::structured::keys_python, reader);
            std::string formatted = self->formatter->format(*type);
            return to_python(formatted);
        } ARKI_CATCH_RETURN_PYO
    }
};


struct FormatterDef : public Type<FormatterDef, arkipy_Formatter>
{
    constexpr static const char* name = "Formatter";
    constexpr static const char* qual_name = "arkimet.Formatter";
    constexpr static const char* doc = R"(
Formatter for arkimet metadata.
)";
    GetSetters<> getsetters;
    Methods<format> methods;

    static void _dealloc(Impl* self)
    {
        delete self->formatter;
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString("Formatter");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromString("Formatter");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            self->formatter = arki::Formatter::create().release();
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

FormatterDef* formatter_def = nullptr;

}

namespace arki {
namespace python {

void register_formatter(PyObject* m)
{
    formatter_def = new FormatterDef;
    formatter_def->define(arkipy_Formatter_Type, m);
}

namespace formatter {

void init()
{
    arki::Formatter::set_factory([] {
        return std::unique_ptr<arki::Formatter>(new PythonFormatter);
    });
}

}
}
}
