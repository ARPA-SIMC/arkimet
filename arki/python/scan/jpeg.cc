#include "jpeg.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/python/common.h"
#include "arki/python/metadata.h"
#include "arki/python/scan.h"
#include "arki/python/utils/methods.h"
#include "arki/python/utils/type.h"
#include "arki/scan/jpeg.h"

namespace arki::python::scan {

/*
 * scan.jpeg module contents
 */

PyObject* jpegscanner_object = nullptr;

static void load_jpegscanner_object()
{
    arki::python::scan::load_scanner_scripts();

    // Get arkimet.scan.nc.BufrScanner
    pyo_unique_ptr module(
        throw_ifnull(PyImport_ImportModule("arkimet.scan.jpeg")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    jpegscanner_object = obj.release();
}

class PythonJPEGScanner : public arki::scan::JPEGScanner
{
protected:
    std::shared_ptr<Metadata>
    scan_jpeg_file(const std::filesystem::path& pathname) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!jpegscanner_object)
            load_jpegscanner_object();

        pyo_unique_ptr pyfname(to_python(pathname));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
            jpegscanner_object, "scan_file", "OO", pyfname.get(), pymd.get())));

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

    std::shared_ptr<Metadata>
    scan_jpeg_data(const std::vector<uint8_t>& data) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        if (!jpegscanner_object)
            load_jpegscanner_object();

        pyo_unique_ptr pydata(to_python(data));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
            jpegscanner_object, "scan_data", "OO", pydata.get(), pymd.get())));

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
    PythonJPEGScanner() {}
    virtual ~PythonJPEGScanner() {}
};

Methods<> jpeg_methods;

extern "C" {

static PyModuleDef jpeg_module = {
    PyModuleDef_HEAD_INIT,
    "jpeg",                            /* m_name */
    "Arkimet JPEG-specific functions", /* m_doc */
    -1,                                /* m_size */
    jpeg_methods.as_py(),              /* m_methods */
    nullptr,                           /* m_slots */
    nullptr,                           /* m_traverse */
    nullptr,                           /* m_clear */
    nullptr,                           /* m_free */
};
}

void register_scan_jpeg(PyObject* scan)
{
    pyo_unique_ptr jpeg = throw_ifnull(PyModule_Create(&jpeg_module));

    if (PyModule_AddObject(scan, "jpeg", jpeg.release()) == -1)
        throw PythonException();
}
void init_scanner_jpeg()
{
    arki::scan::Scanner::register_factory(
        DataFormat::JPEG, [] { return std::make_shared<PythonJPEGScanner>(); });
}
} // namespace arki::python::scan
