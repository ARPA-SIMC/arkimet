#include "netcdf.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/python/common.h"
#include "arki/python/metadata.h"
#include "arki/python/scan.h"
#include "arki/python/utils/methods.h"
#include "arki/python/utils/type.h"
#include "arki/scan/netcdf.h"

namespace arki::python::scan {

PyObject* ncscanner_object = nullptr;

static void load_ncscanner_object()
{
    arki::python::scan::load_scanner_scripts();

    // Get arkimet.scan.nc.BufrScanner
    pyo_unique_ptr module(
        throw_ifnull(PyImport_ImportModule("arkimet.scan.nc")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    ncscanner_object = obj.release();
}

class PythonNetCDFScanner : public arki::scan::NetCDFScanner
{
protected:
    std::shared_ptr<Metadata>
    scan_nc_file(const std::filesystem::path& pathname) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!ncscanner_object)
            load_ncscanner_object();

        pyo_unique_ptr pyfname(to_python(pathname));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
            ncscanner_object, "scan", "OO", pyfname.get(), pymd.get())));

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
    PythonNetCDFScanner() {}
    virtual ~PythonNetCDFScanner() {}
};

Methods<> nc_methods;

extern "C" {

static PyModuleDef nc_module = {
    PyModuleDef_HEAD_INIT,
    "nc",                                /* m_name */
    "Arkimet NetCDF-specific functions", /* m_doc */
    -1,                                  /* m_size */
    nc_methods.as_py(),                  /* m_methods */
    nullptr,                             /* m_slots */
    nullptr,                             /* m_traverse */
    nullptr,                             /* m_clear */
    nullptr,                             /* m_free */
};
}

void register_scan_netcdf(PyObject* scan)
{
    pyo_unique_ptr nc = throw_ifnull(PyModule_Create(&nc_module));

    if (PyModule_AddObject(scan, "nc", nc.release()) == -1)
        throw PythonException();
}
void init_scanner_netcdf()
{
    arki::scan::Scanner::register_factory(DataFormat::NETCDF, [] {
        return std::make_shared<PythonNetCDFScanner>();
    });
}
} // namespace arki::python::scan
