#include "odimh5.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/python/common.h"
#include "arki/python/metadata.h"
#include "arki/python/scan.h"
#include "arki/python/utils/methods.h"
#include "arki/python/utils/type.h"
#include "arki/scan/odimh5.h"

namespace arki::python::scan {

PyObject* odimh5scanner_object = nullptr;

static void load_odimh5scanner_object()
{
    arki::python::scan::load_scanner_scripts();

    // Get arkimet.scan.odimh5.BufrScanner
    pyo_unique_ptr module(
        throw_ifnull(PyImport_ImportModule("arkimet.scan.odimh5")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    odimh5scanner_object = obj.release();
}

class PythonOdimh5Scanner : public arki::scan::OdimScanner
{
protected:
    std::shared_ptr<Metadata>
    scan_h5_file(const std::filesystem::path& pathname) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!odimh5scanner_object)
            load_odimh5scanner_object();

        pyo_unique_ptr pyfname(to_python(pathname));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
            odimh5scanner_object, "scan", "OO", pyfname.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != 1)
            arki::nag::warning(
                "metadata use count after scanning is %ld instead of 1",
                md.use_count());

        return md;
    }

public:
    PythonOdimh5Scanner() {}
    virtual ~PythonOdimh5Scanner() {}
};

Methods<> odimh5_methods;

extern "C" {

static PyModuleDef odimh5_module = {
    PyModuleDef_HEAD_INIT,
    "odimh5",                            /* m_name */
    "Arkimet ODIMh5-specific functions", /* m_doc */
    -1,                                  /* m_size */
    odimh5_methods.as_py(),              /* m_methods */
    nullptr,                             /* m_slots */
    nullptr,                             /* m_traverse */
    nullptr,                             /* m_clear */
    nullptr,                             /* m_free */
};
}

void register_scan_odimh5(PyObject* scan)
{
    pyo_unique_ptr odimh5 = throw_ifnull(PyModule_Create(&odimh5_module));

    if (PyModule_AddObject(scan, "odimh5", odimh5.release()) == -1)
        throw PythonException();
}
void init_scanner_odimh5()
{
    arki::scan::Scanner::register_factory(DataFormat::ODIMH5, [] {
        return std::make_shared<PythonOdimh5Scanner>();
    });
}
} // namespace arki::python::scan
