#include "scan.h"
#include "arki/data/bufr.h"
#include "arki/data/grib.h"
#include "arki/data/jpeg.h"
#include "arki/data/netcdf.h"
#include "arki/defs.h"
#include "arki/libconfig.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/runtime.h"
#include "arki/types/values.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "common.h"
#include "metadata.h"
#include "scan/bufr.h"
#include "scan/grib.h"
#include "scan/jpeg.h"
#include "scan/netcdf.h"
#include "scan/odimh5.h"
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
PyObject* module_arkimet = nullptr;

// Pointer to the scanners module
PyObject* module_scanners = nullptr;

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

struct ScannerDef : public Type<ScannerDef, arkipy_scan_Scanner>
{
    constexpr static const char* name      = "Scanner";
    constexpr static const char* qual_name = "arkimet.scan.Scanner";
    constexpr static const char* doc       = R"(
Scanner for binary data.
)";
    GetSetters<> getsetters;
    Methods<get_scanner, scan_data> methods;

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

    // static int _init(Impl* self, PyObject* args, PyObject* kw)
    // {
    //     static const char* kwlist[] = { nullptr };
    //     if (!PyArg_ParseTupleAndKeywords(args, kw, "",
    //     const_cast<char**>(kwlist)))
    //         return -1;

    //     try {
    //         new(&(self->scanner))
    //         std::shared_ptr<arki::data::Scanner>(make_shared<arki:data::Scanner>());
    //     } ARKI_CATCH_RETURN_INT

    //     return 0;
    // }
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

static PyModuleDef scanners_module = {
    PyModuleDef_HEAD_INIT,
    "scanners",               /* m_name */
    "Arkimet scanner code",   /* m_doc */
    -1,                       /* m_size */
    scanners_methods.as_py(), /* m_methods */
    nullptr,                  /* m_slots */
    nullptr,                  /* m_traverse */
    nullptr,                  /* m_clear */
    nullptr,                  /* m_free */
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
    pyo_unique_ptr scan     = throw_ifnull(PyModule_Create(&scan_module));
    pyo_unique_ptr scanners = throw_ifnull(PyModule_Create(&scanners_module));

    module_arkimet  = m;
    module_scanners = scanners;

    scanner_def = new ScannerDef;
    scanner_def->define(arkipy_scan_Scanner_Type, scan);

    arki::python::scan::register_scan_grib(scan);
    arki::python::scan::register_scan_bufr(scan);
    arki::python::scan::register_scan_odimh5(scan);
    arki::python::scan::register_scan_netcdf(scan);
    arki::python::scan::register_scan_jpeg(scan);
    arki::python::scan::register_scan_vm2(scan);

    if (PyModule_AddObject(scan, "scanners", scanners.release()) == -1)
        throw PythonException();

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

    // Get the name of the scanners module
    if (!module_scanners || !module_arkimet)
        throw std::runtime_error("load_scanner_scripts was called before the "
                                 "_arkimet.scan module has been initialized");

    std::string base = PyModule_GetName(module_arkimet);
    base += ".";
    base += PyModule_GetName(module_scanners);

    std::vector<std::filesystem::path> sources =
        arki::Config::get().dir_scan.list_files(".py");
    for (const auto& source : sources)
    {
        std::string basename = source.filename();

        // Check if the scanner module had already been imported
        std::string module_name =
            base + "." + basename.substr(0, basename.size() - 3);
        pyo_unique_ptr py_module_name(string_to_python(module_name));
        pyo_unique_ptr module(ArkiPyImport_GetModule(py_module_name));
        if (PyErr_Occurred())
            throw PythonException();

        // Import it if needed
        if (!module)
        {
            std::string source_code = utils::sys::read_file(source);
            pyo_unique_ptr code(throw_ifnull(
                Py_CompileStringExFlags(source_code.c_str(), source.c_str(),
                                        Py_file_input, nullptr, -1)));
            module = pyo_unique_ptr(throw_ifnull(
                PyImport_ExecCodeModule(module_name.c_str(), code)));
        }
    }

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

void init()
{
    init_scanner_grib();
    init_scanner_bufr();
    init_scanner_jpeg();
    init_scanner_netcdf();
    init_scanner_odimh5();
}
} // namespace scan

} // namespace arki::python
