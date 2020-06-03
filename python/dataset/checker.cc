#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "python/dataset/checker.h"
#include "python/dataset.h"
#include "python/common.h"
#include "python/metadata.h"
#include "python/summary.h"
#include "python/cfg.h"
#include "python/files.h"
#include "python/matcher.h"
#include "python/utils/values.h"
#include "python/utils/methods.h"
#include "python/utils/type.h"
#include "python/utils/dict.h"
#include "python/dataset/reader.h"
#include "python/dataset/writer.h"
#include "python/dataset/reporter.h"
#include "python/dataset/progress.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/metadata/sort.h"
#include "arki/dataset.h"
#include "arki/dataset/query.h"
#include "arki/dataset/http.h"
#include "arki/dataset/time.h"
#include "arki/dataset/segmented.h"
#include "arki/dataset/session.h"
#include "arki/dataset/progress.h"
#include "arki/nag.h"
#include <ctime>
#include <vector>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetChecker_Type = nullptr;

}

namespace {

/*
 * dataset.Checker
 */

static arki::dataset::CheckerConfig get_checker_config(std::shared_ptr<arki::dataset::Session> session, PyObject* args, PyObject* kw)
{
    static const char* kwlist[] = { "reporter", "segment_filter", "offline", "online", "readonly", "accurate", nullptr };
    PyObject* arg_reporter = nullptr;
    PyObject* arg_segment_filter = nullptr;
    int arg_offline = 1;
    int arg_online = 1;
    int arg_readonly = 1;
    int arg_accurate = 0;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "|OOpppp", const_cast<char**>(kwlist),
            &arg_reporter, &arg_segment_filter,
            &arg_offline, &arg_online, &arg_readonly, &arg_accurate))
        throw PythonException();

    arki::dataset::CheckerConfig cfg;
    if (arg_reporter)
        cfg.reporter = make_shared<python::dataset::ProxyReporter>(arg_reporter);

    if (arg_segment_filter)
        cfg.segment_filter = matcher_from_python(arg_segment_filter);

    cfg.offline = arg_offline;
    cfg.online = arg_online;
    cfg.readonly = arg_readonly;
    cfg.accurate = arg_accurate;

    return cfg;
}

struct repack : public MethKwargs<repack, arkipy_DatasetChecker>
{
    constexpr static const char* name = "repack";
    constexpr static const char* signature = "reporter: Any=None, segment_filter: Union[arkimet.Matcher, str]="", offline: bool=True, online: bool=True, readonly: bool=True, accurate: bool=False";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "Perform repacking on the dataset";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try {
            auto cfg = get_checker_config(self->ptr->dataset().session, args, kw);

            {
                ReleaseGIL gil;
                self->ptr->repack(cfg);
            }

            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct check : public MethKwargs<check, arkipy_DatasetChecker>
{
    constexpr static const char* name = "check";
    constexpr static const char* signature = "reporter: Any=None, segment_filter: Union[arkimet.Matcher, str]="", offline: bool=True, online: bool=True, readonly: bool=True, accurate: bool=False";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "Perform checking/fixing on the dataset";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try {
            auto cfg = get_checker_config(self->ptr->dataset().session, args, kw);

            {
                ReleaseGIL gil;
                self->ptr->check(cfg);
            }

            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct segment_state : public MethKwargs<segment_state, arkipy_DatasetChecker>
{
    constexpr static const char* name = "segment_state";
    constexpr static const char* signature = "reporter: Any=None, segment_filter: Union[arkimet.Matcher, str]="", offline: bool=True, online: bool=True, readonly: bool=True, accurate: bool=False, time_override: int=None";
    constexpr static const char* returns = "Dict[str, str]";
    constexpr static const char* summary = "Compute the state of each segment in the archive";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try {
            auto cfg = get_checker_config(self->ptr->dataset().session, args, kw);

            arki::dataset::segmented::Checker* checker = dynamic_cast<arki::dataset::segmented::Checker*>(self->ptr.get());
            if (!checker)
                Py_RETURN_NONE;

            pyo_unique_ptr res(throw_ifnull(PyDict_New()));

            {
                ReleaseGIL gil;
                checker->segments_recursive(cfg, [&](arki::dataset::segmented::Checker& checker, arki::dataset::segmented::CheckerSegment& segment) {
                    std::string key = checker.name() + ":" + segment.path_relative();
                    auto state = segment.scan(*cfg.reporter, !cfg.accurate);
                    AcquireGIL gil;
                    set_dict(res, key.c_str(), state.state.to_string());
                });
            }
            return res.release();
        } ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetCheckerDef : public Type<DatasetCheckerDef, arkipy_DatasetChecker>
{
    constexpr static const char* name = "Checker";
    constexpr static const char* qual_name = "arkimet.dataset.Checker";
    constexpr static const char* doc = R"(
Check functions for an arkimet dataset.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<MethGenericEnter<Impl>, MethGenericExit<Impl>, repack, check, segment_state> methods;

    static void _dealloc(Impl* self)
    {
        self->ptr.~shared_ptr<arki::dataset::Checker>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Checker(%s, %s)", self->ptr->type().c_str(), self->ptr->name().c_str());
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Checker(%s, %s)", self->ptr->type().c_str(), self->ptr->name().c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "cfg", nullptr };
        PyObject* py_cfg = Py_None;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_cfg))
            return -1;

        try {
            core::cfg::Section cfg;

            if (PyUnicode_Check(py_cfg))
                cfg = arki::dataset::Session::read_config(from_python<std::string>(py_cfg));
            else
                cfg = section_from_python(py_cfg);

            if (PyErr_WarnEx(PyExc_DeprecationWarning, "Use arki.dataset.Session().checker(cfg) instead of arki.dataset.Checker(cfg)", 1))
                return -1;

            auto session = std::make_shared<arki::dataset::Session>();
            new (&(self->ptr)) std::shared_ptr<arki::dataset::Checker>(session->dataset(cfg)->create_checker());
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetCheckerDef* checker_def = nullptr;

}


namespace arki {
namespace python {

void register_dataset_checker(PyObject* module)
{
    checker_def = new DatasetCheckerDef;
    checker_def->define(arkipy_DatasetChecker_Type, module);
}

}
}

