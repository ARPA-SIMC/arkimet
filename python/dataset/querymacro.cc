#include "querymacro.h"
#include "arki/exceptions.h"
#include "arki/querymacro.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include "python/cfg.h"
#include "python/metadata.h"
#include "python/matcher.h"
#include "python/utils/values.h"
#include "python/utils/dict.h"


namespace arki {
namespace python {

namespace {

class PythonMacro : public qmacro::Base
{
    PyObject* qmacro = nullptr;

public:
    PythonMacro(const std::string& source, const qmacro::Options& opts)
        : Base(opts)
    {
        AcquireGIL gil;
        // Check if the qmacro module had already been imported
        std::string module_name = "arki.python.dataset.qmacro." + opts.macro_name;
        pyo_unique_ptr py_module_name(string_to_python(module_name));
        pyo_unique_ptr module(PyImport_GetModule(py_module_name));
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

        qmacro = obj.release();
    }

    ~PythonMacro()
    {
        AcquireGIL gil;
        Py_XDECREF(qmacro);
    }

    bool query_data(const arki::dataset::DataQuery& q, arki::metadata_dest_func dest) override
    {
        AcquireGIL gil;
        pyo_unique_ptr meth(throw_ifnull(PyObject_GetAttrString(qmacro, "query_data")));
        pyo_unique_ptr args(throw_ifnull(PyTuple_New(0)));
        pyo_unique_ptr kwargs(throw_ifnull(PyDict_New()));
        set_dict(kwargs, "matcher", matcher(q.matcher));
        set_dict(kwargs, "with_data", q.with_data);
        // TODO set_dict(kwargs, "sort", q.sorter);
        set_dict(kwargs, "on_metadata", dest);

        pyo_unique_ptr res(throw_ifnull(PyObject_Call(meth, args, kwargs)));
        if (res == Py_None)
            return true;
        return from_python<bool>(res);
    }

    void query_summary(const Matcher& matcher, Summary& summary) override
    {
        AcquireGIL gil;
    //constexpr static const char* signature = "matcher: str=None, summary: arkimet.Summary=None";
    //constexpr static const char* returns = "arkimet.Summary";
        // Pass matcher
        // Pass summary
        // Call the function
    }
};

}

namespace dataset {
namespace qmacro {

void init()
{
    arki::qmacro::register_parser("py", [](const std::string& source, const arki::qmacro::Options& opts) {
        return std::make_shared<PythonMacro>(source, opts);
    });
}

}
}
}
}
