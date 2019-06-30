#include <Python.h>
#include "common.h"
#include "cfg.h"
#include "metadata.h"
#include "summary.h"
#include "dataset.h"
#include "utils/values.h"
#include "utils/methods.h"
#include "arki-query.h"
#include "arki-scan.h"
#include "arki-check.h"
#include "arki-mergeconf.h"
#include "arki-dump.h"
#include "arki-xargs.h"
#include "arki-bufr-prepare.h"
#include "arki/matcher.h"
#include "arki/runtime.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/http.h"
#include "arki/querymacro.h"
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

struct matcher_alias_database : public MethNoargs<matcher_alias_database, PyObject>
{
    constexpr static const char* name = "matcher_alias_database";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "str";
    constexpr static const char* summary = "return a string with the current matcher alias database";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self)
    {
        try {
            core::cfg::Sections cfg = MatcherAliasDatabase::serialise();
            stringstream ss;
            cfg.write(ss, "memory");
            return to_python(ss.str());
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

            unique_ptr<dataset::Reader> ds;
            string baseurl = dataset::http::Reader::allSameRemoteServer(datasets);
            if (baseurl.empty())
            {
                // Create the local query macro
                ds.reset(new Querymacro(cfg, datasets, name, query));
            } else {
                // Create the remote query macro
                core::cfg::Section cfg;
                cfg.set("name", name);
                cfg.set("type", "remote");
                cfg.set("path", baseurl);
                cfg.set("qmacro", query);
                ds = dataset::Reader::create(cfg);
            }

            return (PyObject*)dataset_reader_create(move(ds));
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

            std::unique_ptr<dataset::Merged> ds(new dataset::Merged);
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

Methods<expand_query, matcher_alias_database, make_merged_dataset, make_qmacro_dataset, get_version> methods;

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

PyMODINIT_FUNC PyInit__arkimet(void)
{
    using namespace arki::python;

    arki::runtime::init();

    PyObject* m = PyModule_Create(&arkimet_module);
    if (!m) return m;

    try {
        register_cfg(m);
        register_metadata(m);
        register_summary(m);
        register_dataset(m);
        register_arki_query(m);
        register_arki_scan(m);
        register_arki_check(m);
        register_arki_mergeconf(m);
        register_arki_dump(m);
        register_arki_xargs(m);
        register_arki_bufr_prepare(m);
    } catch (PythonException&) {
        return nullptr;
    }

    return m;
}

}
