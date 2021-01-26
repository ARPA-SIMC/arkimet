#include "querymacro.h"
#include "arki/dataset/querymacro.h"
#include "arki/utils/sys.h"
#include "python/common.h"
#include "python/utils/values.h"
#include "python/dataset/python.h"
#include "python/dataset/session.h"


namespace arki {
namespace python {

namespace {

PyObject* instantiate_qmacro_pydataset(const std::string& source, std::shared_ptr<arki::dataset::QueryMacro> dataset)
{
    // Check if the qmacro module had already been imported
    std::string module_name = "arki.python.dataset.qmacro." + dataset->name();
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

    // Create a python proxy for the dataset session
    pyo_unique_ptr session((PyObject*)dataset_session_create(dataset->session, dataset->pool));

    // Instantiate obj = Querymacro(macro_cfg, datasets_cfg, args, query)
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, "Os#s#",
                    session.get(),
                    dataset->macro_args.data(), (Py_ssize_t)dataset->macro_args.size(),
                    dataset->query.data(), (Py_ssize_t)dataset->query.size())));

    return obj.release();
}

}

namespace dataset {
namespace qmacro {

void init()
{
    arki::dataset::qmacro::register_parser("py", [](const std::string& source, std::shared_ptr<arki::dataset::QueryMacro> datasets) {
        AcquireGIL gil;
        pyo_unique_ptr o(instantiate_qmacro_pydataset(source, datasets));
        return python::dataset::create_reader(datasets->session, o);
    });
}

}
}
}
}
