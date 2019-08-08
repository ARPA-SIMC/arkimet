#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "common.h"
#include "cfg.h"
#include "metadata.h"
#include "summary.h"
#include "dataset.h"
#include "matcher.h"
#include "counters.h"
#include "formatter.h"
#include "utils/values.h"
#include "utils/methods.h"
#include "utils/dict.h"
#include "dataset/querymacro.h"
#include "arki-query.h"
#include "arki-scan.h"
#include "arki-check.h"
#include "arki-dump.h"
#include "arki-xargs.h"
#include "arki-bufr-prepare.h"
#include "arki/matcher/aliases.h"
#include "arki/runtime.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/http.h"
#include "arki/querymacro.h"
#include "arki/nag.h"
#include "arki/libconfig.h"
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::python;

namespace {

struct expand_query : public MethKwargs<expand_query, PyObject>
{
    constexpr static const char* name = "expand_query";
    constexpr static const char* signature = "query: str";
    constexpr static const char* returns = "str";
    constexpr static const char* summary = "expand aliases in an Arkimet query, returning the same query without use of aliases";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "query", nullptr };
        const char* query = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s", const_cast<char**>(kwlist), &query))
            return nullptr;

        try {
            Matcher m = Matcher::parse(query);
            return to_python(m.toStringExpanded());
        } ARKI_CATCH_RETURN_PYO
    }
};

struct get_alias_database : public MethNoargs<get_alias_database, PyObject>
{
    constexpr static const char* name = "get_alias_database";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "arkimet.cfg.Sections";
    constexpr static const char* summary = "return a the current matcher alias database";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            return cfg_sections(matcher::AliasDatabase::serialise());
        } ARKI_CATCH_RETURN_PYO
    }
};

struct make_qmacro_dataset : public MethKwargs<make_qmacro_dataset, PyObject>
{
    constexpr static const char* name = "make_qmacro_dataset";
    constexpr static const char* signature = "cfg: Union[str, dict, arkimet.cfg.Section], datasets: Union[str, arkimet.cfg.Sections], name: str, query: str";
    constexpr static const char* returns = "arkimet.DatasetReader";
    constexpr static const char* summary = "create a QueryMacro dataset that aggregates the contents of multiple datasets";
    constexpr static const char* doc = R"(
Arguments:
  cfg: base configuration of the resulting dataset
  datasets: configuration of all the available datasets
  name: name of the query macro to use
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "cfg", "datasets", "name", "query", NULL };
        PyObject* arg_cfg = Py_None;
        PyObject* arg_datasets = Py_None;
        const char* name = nullptr;
        const char* query = "";

        if (!PyArg_ParseTupleAndKeywords(args, kw, "OOs|s", (char**)kwlist, &arg_cfg, &arg_datasets, &name, &query))
            return nullptr;

        try {
            core::cfg::Section cfg = section_from_python(arg_cfg);
            core::cfg::Sections datasets;
            datasets = sections_from_python(arg_datasets);

            std::shared_ptr<arki::dataset::Reader> ds;
            string baseurl = arki::dataset::http::Reader::allSameRemoteServer(datasets);
            if (baseurl.empty())
            {
                // Create the local query macro
                arki::qmacro::Options opts(cfg, datasets, name, query);
                ds = qmacro::get(opts);
            } else {
                // Create the remote query macro
                core::cfg::Section cfg;
                cfg.set("name", name);
                cfg.set("type", "remote");
                cfg.set("path", baseurl);
                cfg.set("qmacro", query);
                ds = arki::dataset::Reader::create(cfg);
            }

            return (PyObject*)dataset_reader_create(ds);
        } ARKI_CATCH_RETURN_PYO
    }
};

struct make_merged_dataset : public MethKwargs<make_merged_dataset, PyObject>
{
    constexpr static const char* name = "make_merged_dataset";
    constexpr static const char* signature = "cfg: Union[str, arkimet.cfg.Sections]";
    constexpr static const char* returns = "arkimet.DatasetReader";
    constexpr static const char* summary = "create a merged dataset from all the datasets found in the given configuration";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "cfg", NULL };
        PyObject* arg_cfg = Py_None;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", (char**)kwlist, &arg_cfg))
            return nullptr;

        try {
            core::cfg::Sections cfg = sections_from_python(arg_cfg);

            std::unique_ptr<arki::dataset::Merged> ds(new arki::dataset::Merged);
            for (auto si: cfg)
                ds->add_dataset(si.second);
            return (PyObject*)dataset_reader_create(move(ds));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct get_version : public MethNoargs<get_version, PyObject>
{
    constexpr static const char* name = "get_version";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "str";
    constexpr static const char* summary = "get a string with the current Arkimet version";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            return to_python(std::string(PACKAGE_VERSION));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct set_verbosity : public MethKwargs<set_verbosity, PyObject>
{
    constexpr static const char* name = "set_verbosity";
    constexpr static const char* signature = "verbose: bool=False, debug: bool=False";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "set the arkimet warning verbosity";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "verbose", "debug", nullptr };
        int verbose = 0;
        int debug = 0;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "pp", const_cast<char**>(kwlist), &verbose, &debug))
            return nullptr;

        try {
            arki::nag::init(verbose, debug);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct config : public MethNoargs<config, PyObject>
{
    constexpr static const char* name = "config";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "Dict[str, Dict[str, str]]";
    constexpr static const char* summary = "return the arkimet runtime configuration";
    constexpr static const char* doc = nullptr;

    static pyo_unique_ptr describe_dirlist(const arki::Config::Dirlist& dirlist, const char* desc, const char* envvar)
    {
        pyo_unique_ptr result(throw_ifnull(PyDict_New()));
        set_dict(result, "dirs", dirlist);
        set_dict(result, "desc", desc);
        if (envvar)
            set_dict(result, "env", envvar);
        return result;
    }

    static pyo_unique_ptr describe_string(const std::string& value, const char* desc, const char* envvar)
    {
        pyo_unique_ptr result(throw_ifnull(PyDict_New()));
        set_dict(result, "value", value);
        set_dict(result, "desc", desc);
        if (envvar)
            set_dict(result, "env", envvar);
        return result;
    }

    static PyObject* run(Impl* self)
    {
        try {
            auto& cfg = arki::Config::get();
            pyo_unique_ptr result(throw_ifnull(PyDict_New()));
            set_dict(result, "format", describe_dirlist(cfg.dir_formatter, "Formatter scripts", "ARKI_FORMATTER"));
            set_dict(result, "bbox", describe_dirlist(cfg.dir_bbox, "Bounding box scripts", "ARKI_BBOX"));
            set_dict(result, "postproc", describe_dirlist(cfg.dir_postproc, "Postprocessors", "ARKI_POSTPROC"));
            set_dict(result, "report", describe_dirlist(cfg.dir_report, "Report scripts", "ARKI_REPORT"));
            set_dict(result, "qmacro", describe_dirlist(cfg.dir_qmacro, "Query macro scripts", "ARKI_QMACRO"));
            set_dict(result, "scan_grib1", describe_dirlist(cfg.dir_scan_grib1, "GRIB1 scan scripts", "ARKI_SCAN_GRIB1"));
            set_dict(result, "scan_grib2", describe_dirlist(cfg.dir_scan_grib2, "GRIB2 scan scripts", "ARKI_SCAN_GRIB2"));
            set_dict(result, "scan_odimh5", describe_dirlist(cfg.dir_scan_odimh5, "ODIMH5 scan scripts", "ARKI_SCAN_ODIMH5"));
            set_dict(result, "bufr", describe_dirlist(cfg.dir_scan_bufr, "BUFR scan scripts", "ARKI_SCAN_BUFR"));
            set_dict(result, "iotrace", describe_string(cfg.file_iotrace_output, "I/O profiling log file", "ARKI_IOTRACE"));
            set_dict(result, "vm2_config", describe_string(cfg.file_vm2_config, "VM2 configuration file", "ARKI_VM2_FILE"));
            return result.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct debug_tty : public MethKwargs<debug_tty, PyObject>
{
    constexpr static const char* name = "debug_tty";
    constexpr static const char* signature = "text: str";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "write a debug message to /dev/tty";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "text", nullptr };
        const char* text = nullptr;
        Py_ssize_t text_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &text, &text_len))
            return nullptr;

        try {
            arki::nag::debug_tty("%.*s", (int)text_len, text);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

Methods<expand_query, get_alias_database, make_merged_dataset, make_qmacro_dataset, get_version, set_verbosity, config, debug_tty> methods;

void add_feature(PyObject* set, const char* feature)
{
    pyo_unique_ptr name(to_python(feature));
    if (PySet_Add(set, name) == -1)
        throw PythonException();
}

struct PythonNagHandler : public nag::Handler
{
    PyObject* py_warning = nullptr;
    PyObject* py_info = nullptr;
    PyObject* py_debug = nullptr;

    PythonNagHandler()
    {
        // import logging
        pyo_unique_ptr mod_logging(throw_ifnull(PyImport_ImportModule("logging")));
        // logger = logging.getLogger("arkimet")
        pyo_unique_ptr logger(throw_ifnull(PyObject_CallMethod(mod_logging, "getLogger", "s", "arkimet")));

        pyo_unique_ptr meth_warning(throw_ifnull(PyObject_GetAttrString(logger, "warning")));
        pyo_unique_ptr meth_info(throw_ifnull(PyObject_GetAttrString(logger, "info")));
        pyo_unique_ptr meth_debug(throw_ifnull(PyObject_GetAttrString(logger, "debug")));

        py_warning = meth_warning.release();
        py_info = meth_info.release();
        py_debug = meth_debug.release();
    }

    ~PythonNagHandler()
    {
        Py_XDECREF(py_warning);
        Py_XDECREF(py_info);
        Py_XDECREF(py_debug);
    }

    void warning(const char* fmt, va_list ap) override
    {
        std::string msg = format(fmt, ap);
        AcquireGIL gil;
        throw_ifnull(PyObject_CallFunction(
                    py_warning, "ss#", "%s", msg.data(), (Py_ssize_t)msg.size()));
    }

    void verbose(const char* fmt, va_list ap) override
    {
        std::string msg = format(fmt, ap);
        AcquireGIL gil;
        throw_ifnull(PyObject_CallFunction(
                    py_info, "ss#", "%s", msg.data(), (Py_ssize_t)msg.size()));
    }

    void debug(const char* fmt, va_list ap) override
    {
        std::string msg = format(fmt, ap);
        AcquireGIL gil;
        throw_ifnull(PyObject_CallFunction(
                    py_debug, "ss#", "%s", msg.data(), (Py_ssize_t)msg.size()));
    }
};

}

extern "C" {

static PyModuleDef arkimet_module = {
    PyModuleDef_HEAD_INIT,
    "_arkimet",       /* m_name */
    "Arkimet Python interface.",  /* m_doc */
    -1,               /* m_size */
    methods.as_py(),  /* m_methods */
    NULL,             /* m_slots */
    NULL,             /* m_traverse */
    NULL,             /* m_clear */
    NULL,             /* m_free */

};

arki::nag::Handler* python_nag_handler = nullptr;

PyMODINIT_FUNC PyInit__arkimet(void)
{
    using namespace arki::python;

    arki::init();

    arki::python::formatter::init();
    arki::python::dataset::qmacro::init();

    python_nag_handler = new PythonNagHandler;
    python_nag_handler->install();

    PyObject* m = PyModule_Create(&arkimet_module);
    if (!m) return m;

    try {
        pyo_unique_ptr features(throw_ifnull(PyFrozenSet_New(nullptr)));

#ifdef HAVE_DBALLE
        add_feature(features, "dballe");
#endif
#ifdef HAVE_GEOS
        add_feature(features, "geos");
#endif
#ifdef HAVE_GRIBAPI
        add_feature(features, "grib");
#endif
#ifdef HAVE_HDF5
        add_feature(features, "hdf5");
#endif
#ifdef HAVE_VM2
        add_feature(features, "vm2");
#endif
#ifdef HAVE_CURL
        add_feature(features, "curl");
#endif
#ifdef HAVE_LUA
        add_feature(features, "lua");
#endif
#ifdef HAVE_LIBARCHIVE
        add_feature(features, "libarchive");
#endif
#ifdef HAVE_LIBZIP
        add_feature(features, "libzip");
#endif
#ifdef HAVE_SQLITE3
        add_feature(features, "sqlite");
#endif
#ifdef HAVE_SPLICE
        add_feature(features, "splice");
#endif
#ifdef HAVE_IOTRACE
        add_feature(features, "iotrace");
#endif

        if (PyModule_AddObject(m, "features", features.release()) == -1)
            throw PythonException();

        register_cfg(m);
        register_formatter(m);
        register_metadata(m);
        register_summary(m);
        register_matcher(m);
        register_dataset(m);
        register_counters(m);
        register_arki_query(m);
        register_arki_scan(m);
        register_arki_check(m);
        register_arki_dump(m);
        register_arki_xargs(m);
        register_arki_bufr_prepare(m);
    } ARKI_CATCH_RETURN_PYO

    return m;
}

}
