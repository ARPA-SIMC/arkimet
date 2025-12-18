#include "bufr.h"
#include "arki/data/bufr.h"
#include "arki/libconfig.h"
#include "arki/nag.h"
#include "arki/python/common.h"
#include "arki/python/metadata.h"
#include "arki/python/scan.h"
#include "arki/python/utils/methods.h"
#include "arki/python/utils/type.h"

#ifdef HAVE_DBALLE
#include "arki/python/utils/dballe.h"
#include "arki/python/utils/wreport.h"
#include <dballe/message.h>
#include <dballe/var.h>
#include <wreport/python.h>
#endif

extern "C" {
PyTypeObject* arkipy_scan_BufrMessage_Type = nullptr;
}

namespace arki::python::scan {

#ifdef HAVE_DBALLE
// Wreport API
arki::python::Wreport wreport_api;

// Dballe API
arki::python::Dballe dballe_api;

PyObject* bufrscanner_object = nullptr;

static void load_bufrscanner_object()
{
    load_scanner_scripts();

    // Get arkimet.scan.bufr.BufrScanner
    pyo_unique_ptr module(
        throw_ifnull(PyImport_ImportModule("arkimet.scan.bufr")));
    pyo_unique_ptr cls(throw_ifnull(PyObject_GetAttrString(module, "Scanner")));
    pyo_unique_ptr obj(throw_ifnull(PyObject_CallFunction(cls, nullptr)));

    // Hold a reference to arki.python.BBox forever once loaded the first time
    bufrscanner_object = obj.release();
}

class PythonBufrScanner : public arki::data::BufrScanner
{
protected:
    void scan_extra(dballe::BinaryMessage& rmsg,
                    std::shared_ptr<dballe::Message> msg,
                    std::shared_ptr<Metadata> md) override
    {
        auto orig_use_count = md.use_count();

        AcquireGIL gil;
        if (!bufrscanner_object)
            load_bufrscanner_object();

        pyo_unique_ptr pymsg(dballe_api.message_create(msg));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
            bufrscanner_object, "scan", "OO", pymsg.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != orig_use_count)
            arki::nag::warning(
                "metadata use count after scanning is %ld instead of %ld",
                md.use_count(), orig_use_count);
    }

public:
    PythonBufrScanner() {}
    virtual ~PythonBufrScanner() {}
};
#endif

Methods<> bufr_methods;

extern "C" {
static PyModuleDef bufr_module = {
    PyModuleDef_HEAD_INIT,
    "bufr",                            /* m_name */
    "Arkimet BUFR-specific functions", /* m_doc */
    -1,                                /* m_size */
    bufr_methods.as_py(),              /* m_methods */
    nullptr,                           /* m_slots */
    nullptr,                           /* m_traverse */
    nullptr,                           /* m_clear */
    nullptr,                           /* m_free */
};
}

void register_scan_bufr(PyObject* scan)
{
#ifdef HAVE_DBALLE
    wreport_api.import();
    dballe_api.import();
#endif
    pyo_unique_ptr bufr = throw_ifnull(PyModule_Create(&bufr_module));
    if (PyModule_AddObject(scan, "bufr", bufr.release()) == -1)
        throw PythonException();
}
void init_scanner_bufr()
{
#ifdef HAVE_DBALLE
    arki::data::Scanner::register_factory(
        DataFormat::BUFR, [] { return std::make_shared<PythonBufrScanner>(); });
#endif
}
} // namespace arki::python::scan
