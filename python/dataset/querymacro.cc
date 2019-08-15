#include "querymacro.h"
#include "arki/exceptions.h"
#include "arki/querymacro.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include "arki/summary.h"
#include "arki/sort.h"
#include "python/common.h"
#include "python/cfg.h"
#include "python/metadata.h"
#include "python/matcher.h"
#include "python/summary.h"
#include "python/utils/values.h"
#include "python/utils/dict.h"
#include "python/dataset/python.h"


namespace arki {
namespace python {

namespace {

PyObject* instantiate_qmacro_pydataset(const std::string& source, const qmacro::Options& opts)
{
    // Check if the qmacro module had already been imported
    std::string module_name = "arki.python.dataset.qmacro." + opts.macro_name;
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

    // Get module.Querymacro
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Querymacro")));

    pyo_unique_ptr macro_cfg(cfg_section(opts.macro_cfg));
    pyo_unique_ptr datasets_cfg(cfg_sections(opts.datasets_cfg));

    // Instantiate obj = Querymacro(macro_cfg, datasets_cfg, args, query)
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, "OOs#s#",
                    macro_cfg.get(),
                    datasets_cfg.get(),
                    opts.macro_args.data(), (Py_ssize_t)opts.macro_args.size(),
                    opts.query.data(), (Py_ssize_t)opts.query.size())));

    return obj.release();
};

}

namespace dataset {
namespace qmacro {

void init()
{
    fprintf(stderr, "QUERYMACRO PYTHON INIT CALLED\n");
    arki::qmacro::register_parser("py", [](const std::string& source, const arki::qmacro::Options& opts) {
        AcquireGIL gil;
        pyo_unique_ptr o(instantiate_qmacro_pydataset(source, opts));
        return python::dataset::create_reader(opts.macro_cfg, o);
    });
}

}
}
}
}
