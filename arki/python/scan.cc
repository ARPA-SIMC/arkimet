#include "scan.h"
#include "arki/data.h"
#include "arki/data/bufr.h"
#include "arki/data/grib.h"
#include "arki/defs.h"
#include "arki/libconfig.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/nag.h"
#include "arki/runtime.h"
#include "arki/types/values.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "common.h"
#include "metadata.h"
#include "scan/bufr.h"
#include "scan/grib.h"
#include "scan/vm2.h"
#include "structured.h"
#include "utils/methods.h"
#include "utils/type.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::python;

extern "C" {
PyTypeObject* arkipy_scan_Scanner_Type = nullptr;
}

namespace {

// Pointer to the arkimet module
PyObject* module_arkimet      = nullptr;
// Pointer to the arkimet.scan module
PyObject* module_arkimet_scan = nullptr;

Methods<> scan_methods;
Methods<> scanners_methods;

struct get_scanner : public ClassMethKwargs<get_scanner>
{
    constexpr static const char* name      = "get_scanner";
    constexpr static const char* signature = "format: str";
    constexpr static const char* returns   = "arkimet.scan.Scanner";
    constexpr static const char* summary =
        "Return a Scanner for the given data format";

    static PyObject* run(PyTypeObject* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"format", nullptr};
        PyObject* arg_format        = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", pass_kwlist(kwlist),
                                         &arg_format))
            return nullptr;

        try
        {
            auto scanner =
                arki::data::Scanner::get(dataformat_from_python(arg_format));
            return (PyObject*)arki::python::scan::scanner_create(scanner);
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct scan_data : public MethKwargs<scan_data, arkipy_scan_Scanner>
{
    constexpr static const char* name      = "scan_data";
    constexpr static const char* signature = "data: bytes";
    constexpr static const char* returns   = "arkimet.Metadata";
    constexpr static const char* summary   = "Scan a memory buffer";
    constexpr static const char* doc       = R"(
Returns a Metadata with inline source.
)";
    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"data", nullptr};
        PyObject* arg_data          = nullptr;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", pass_kwlist(kwlist),
                                         &arg_data))
            return nullptr;

        try
        {
            char* buffer;
            Py_ssize_t length;
            if (PyBytes_Check(arg_data))
            {
                if (PyBytes_AsStringAndSize(arg_data, &buffer, &length) == -1)
                    throw PythonException();
            }
            else
            {
                PyErr_Format(PyExc_TypeError,
                             "data has type %R instead of bytes", arg_data);
                return nullptr;
            }

            // FIXME: memory copy, seems unavoidable at the moment
            std::vector<uint8_t> data(buffer, buffer + length);
            auto md = self->scanner->scan_data(data);
            return (PyObject*)metadata_create(md);
        }
        ARKI_CATCH_RETURN_PYO
    }
};

class PythonScanner : public arki::data::SingleFileScanner
{
    DataFormat m_format;
    PyObject* scanner;

public:
    PythonScanner(DataFormat format, PyObject* scanner)
        : m_format(format), scanner(scanner)
    {
        Py_XINCREF(scanner);
    }
    ~PythonScanner()
    {
        // Here we leak the object, never decrementing the reference count that
        // we own: this is because PythonScanner's destructor may get called
        // after the Python interpreter has shut down.
        //
        // We could find a way to check if the interpreter is still running,
        // but since scanners are installed once and kept for the whole
        // duration of the process, we don't really have to, and we can keep
        // the reference forever.
        //
        // AcquireGIL gil;
        // Py_XDECREF(scanner);
    }
    PythonScanner(const PythonScanner&)            = delete;
    PythonScanner(PythonScanner&&)                 = delete;
    PythonScanner& operator=(const PythonScanner&) = delete;
    PythonScanner& operator=(PythonScanner&&)      = delete;

    DataFormat name() const override { return m_format; }

    std::shared_ptr<Metadata>
    scan_file_single(const std::filesystem::path& abspath) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        pyo_unique_ptr pyfname(to_python(abspath));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
            scanner, "scan_file", "OO", pyfname.get(), pymd.get())));

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

    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override
    {
        // Read all in a buffer
        std::vector<uint8_t> buf;
        const unsigned blocksize = 4096;
        while (true)
        {
            buf.resize(buf.size() + blocksize);
            unsigned read =
                in.read(buf.data() + buf.size() - blocksize, blocksize);
            if (read < blocksize)
            {
                buf.resize(buf.size() - blocksize + read);
                break;
            }
        }

        return dest(scan_data(buf));
    }

    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override
    {
        auto md = std::make_shared<Metadata>();

        AcquireGIL gil;
        pyo_unique_ptr pydata(to_python(data));
        pyo_unique_ptr pymd((PyObject*)metadata_create(md));
        pyo_unique_ptr obj(throw_ifnull(PyObject_CallMethod(
            scanner, "scan_data", "OO", pydata.get(), pymd.get())));

        // If use_count is > 1, it means we are potentially and unexpectedly
        // holding all the metadata (and potentially their data) in memory,
        // while a supported and important use case is to stream out one
        // metadata at a time
        pymd.reset(nullptr);
        if (md.use_count() != 1)
            arki::nag::warning(
                "metadata use count after scanning is %ld instead of 1",
                md.use_count());

        md->set_source_inline(m_format,
                              metadata::DataManager::get().to_data(
                                  m_format, std::vector<uint8_t>(data)));

        return md;
    }
};

struct register_scanner : public ClassMethKwargs<register_scanner>
{
    constexpr static const char* name = "register_scanner";
    constexpr static const char* signature =
        "data_format: str, scanner: DataFormatScannner";
    constexpr static const char* returns = "None";
    constexpr static const char* summary =
        "Register a scanner for the given data format";

    static PyObject* run(arkipy_scan_Scanner* cls, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"data_format", "scanner", nullptr};
        PyObject* arg_data_format   = nullptr;
        PyObject* arg_scanner       = nullptr;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "OO", pass_kwlist(kwlist),
                                         &arg_data_format, &arg_scanner))
            return nullptr;

        try
        {
            DataFormat data_format = from_python<DataFormat>(arg_data_format);
            auto scanner =
                std::make_shared<PythonScanner>(data_format, arg_scanner);

            data::Scanner::register_scanner(data_format, scanner);

            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct ScannerDef : public Type<ScannerDef, arkipy_scan_Scanner>
{
    constexpr static const char* name      = "Scanner";
    constexpr static const char* qual_name = "arkimet.scan.Scanner";
    constexpr static const char* doc       = R"(
Scanner for binary data.
)";
    GetSetters<> getsetters;
    Methods<get_scanner, scan_data, register_scanner> methods;

    static void _dealloc(Impl* self)
    {
        self->scanner.~shared_ptr();
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        std::string str = "scanner:" + format_name(self->scanner->name());
        return PyUnicode_FromStringAndSize(str.data(), str.size());
    }

    static PyObject* _repr(Impl* self)
    {
        std::string str = "scanner:" + format_name(self->scanner->name());
        return PyUnicode_FromStringAndSize(str.data(), str.size());
    }
};

ScannerDef* scanner_def = nullptr;

} // namespace

extern "C" {

static PyModuleDef scan_module = {
    PyModuleDef_HEAD_INIT,
    "scan",                              /* m_name */
    "Arkimet format-specific functions", /* m_doc */
    -1,                                  /* m_size */
    scan_methods.as_py(),                /* m_methods */
    nullptr,                             /* m_slots */
    nullptr,                             /* m_traverse */
    nullptr,                             /* m_clear */
    nullptr,                             /* m_free */
};
}

namespace arki::python {

static pyo_unique_ptr data_formats_tuple()
{
    /// Build the data_formats tuple
    pyo_unique_ptr data_formats(throw_ifnull(
        PyTuple_New(static_cast<unsigned>(DataFormat::__END__) - 1)));
    for (unsigned i = static_cast<unsigned>(DataFormat::GRIB);
         i < static_cast<unsigned>(DataFormat::__END__); ++i)
    {
        std::string name = format_name(static_cast<DataFormat>(i));
        PyTuple_SET_ITEM(data_formats, i - 1, to_python(name));
    }
    return data_formats;
}

void register_scan(PyObject* m)
{
    pyo_unique_ptr scan = throw_ifnull(PyModule_Create(&scan_module));

    module_arkimet      = m;
    module_arkimet_scan = scan.get();

    scanner_def = new ScannerDef;
    scanner_def->define(arkipy_scan_Scanner_Type, scan);

    arki::python::scan::register_scan_grib(scan);
    arki::python::scan::register_scan_bufr(scan);
    arki::python::scan::register_scan_vm2(scan);

    pyo_unique_ptr data_formats(data_formats_tuple());
    if (PyModule_AddObject(scan, "data_formats", data_formats.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(m, "scan", scan.release()) == -1)
        throw PythonException();
};

namespace scan {

/// Load scripts from the dir_scan configuration directory
void load_scanner_scripts()
{
    using namespace arki;

    static bool scanners_loaded = false;

    if (scanners_loaded)
        return;

    init_scanner_grib();
    init_scanner_bufr();

    AcquireGIL gil;
    // Import arkimet.scan
    pyo_unique_ptr arkimet_scan(
        throw_ifnull(PyImport_ImportModule("arkimet.scan")));
    // Access arkimet.scan.registry
    pyo_unique_ptr registry(
        throw_ifnull(PyObject_GetAttrString(arkimet_scan, "registry")));
    // Call registry.ensure_loaded()
    throw_ifnull(PyObject_CallMethod(registry, "ensure_loaded", NULL));

    scanners_loaded = true;
}

arkipy_scan_Scanner*
scanner_create(std::shared_ptr<arki::data::Scanner> scanner)
{
    arkipy_scan_Scanner* result =
        PyObject_New(arkipy_scan_Scanner, arkipy_scan_Scanner_Type);
    if (!result)
        throw PythonException();
    new (&(result->scanner)) std::shared_ptr<arki::data::Scanner>(scanner);
    return result;
}

void init() { data::Scanner::register_init(load_scanner_scripts); }
} // namespace scan

} // namespace arki::python
