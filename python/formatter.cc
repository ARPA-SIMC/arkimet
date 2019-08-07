#include "formatter.h"
#include "structured.h"
#include "arki/formatter.h"
#include "arki/types.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "python/utils/values.h"
#include "python/common.h"

using namespace arki::utils;

namespace arki {
namespace python {

namespace {

PyObject* formatter_object = nullptr;

void load_formatter_object()
{
    std::vector<std::string> sources = arki::Config::get().dir_formatter.list_files(".py");
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
    std::string operator()(const types::Type& v) const override
    {
        AcquireGIL gil;
        if (!formatter_object)
            load_formatter_object();

        arki::python::PythonEmitter e;
        v.serialise(e, structured::keys_python);

        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
                        formatter_object, "format", "O", e.res.get())));

        if (obj == Py_None)
            return v.to_string();

        return from_python<std::string>(obj);
    }
};

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
