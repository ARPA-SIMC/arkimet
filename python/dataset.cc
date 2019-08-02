#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "dataset.h"
#include "common.h"
#include "metadata.h"
#include "summary.h"
#include "cfg.h"
#include "files.h"
#include "matcher.h"
#include "utils/values.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/dict.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/dataset.h"
#include "arki/dataset/http.h"
#include "arki/dataset/time.h"
#include "arki/dataset/segmented.h"
#include "arki/nag.h"
#include "dataset/reporter.h"
#include "arki/sort.h"
#include <vector>
#include "config.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetReader_Type = nullptr;
PyTypeObject* arkipy_DatasetWriter_Type = nullptr;
PyTypeObject* arkipy_DatasetChecker_Type = nullptr;
PyTypeObject* arkipy_DatasetSessionTimeOverride_Type = nullptr;

PyObject* arkipy_ImportError = nullptr;
PyObject* arkipy_ImportDuplicateError = nullptr;
PyObject* arkipy_ImportFailedError = nullptr;

}

namespace {


/*
 * dataset.Reader
 */

struct query_data : public MethKwargs<query_data, arkipy_DatasetReader>
{
    constexpr static const char* name = "query_data";
    constexpr static const char* signature = "matcher: Union[arki.Matcher, str]=None, with_data: bool=False, sort: str=None, on_metadata: Callable[[metadata], Optional[bool]]=None";
    constexpr static const char* returns = "None";
    constexpr static const char* summary = "query a dataset, processing the resulting metadata one by one";
    constexpr static const char* doc = R"(
Arguments:
  matcher: the matcher string to filter data to return.
  with_data: if True, also load data together with the metadata.
  sort: string with the desired sort order of results.
  on_metadata: a function called on each metadata, with the Metadata
               object as its only argument. Return None or True to
               continue processing results, False to stop.
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "matcher", "with_data", "sort", "on_metadata", nullptr };
        PyObject* arg_matcher = Py_None;
        PyObject* arg_with_data = Py_None;
        PyObject* arg_sort = Py_None;
        PyObject* arg_on_metadata = Py_None;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "|OOOO", const_cast<char**>(kwlist), &arg_matcher, &arg_with_data, &arg_sort, &arg_on_metadata))
            return nullptr;

        try {
            arki::dataset::DataQuery query(from_python<arki::Matcher>(arg_matcher));
            if (arg_with_data != Py_None)
                query.with_data = from_python<bool>(arg_with_data);
            string sort;
            if (arg_sort != Py_None)
                sort = string_from_python(arg_sort);
            if (!sort.empty()) query.sorter = sort::Compare::parse(sort);

            metadata_dest_func dest;
            pyo_unique_ptr res_list;
            if (arg_on_metadata == Py_None)
            {
                res_list.reset(throw_ifnull(PyList_New(0)));

                dest = [&](std::unique_ptr<Metadata> md) {
                    AcquireGIL gil;
                    pyo_unique_ptr py_md((PyObject*)throw_ifnull(metadata_create(std::move(md))));
                    if (PyList_Append(res_list, py_md) == -1)
                        throw PythonException();
                    return true;
                };
            } else
                dest = dest_func_from_python(arg_on_metadata);

            bool res;
            {
                ReleaseGIL gil;
                res = self->ds->query_data(query, dest);
            }

            if (res_list)
                return res_list.release();
            if (res)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_summary : public MethKwargs<query_summary, arkipy_DatasetReader>
{
    constexpr static const char* name = "query_summary";
    constexpr static const char* signature = "matcher: Union[arki.Matcher, str]=None, summary: arkimet.Summary=None";
    constexpr static const char* returns = "arkimet.Summary";
    constexpr static const char* summary = "query a dataset, returning an arkimet.Summary with the results";
    constexpr static const char* doc = R"(
Arguments:
  matcher: the matcher string to filter data to return.
  summary: not None, add results to this arkimet.Summary, and return
           it, instead of creating a new one.
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "matcher", "summary", NULL };
        PyObject* arg_matcher = Py_None;
        PyObject* arg_summary = Py_None;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "|OO", const_cast<char**>(kwlist), &arg_matcher, &arg_summary))
            return nullptr;

        try {
            auto matcher = matcher_from_python(arg_matcher);

            Summary* summary = nullptr;
            if (arg_summary != Py_None)
            {
                if (!arkipy_Summary_Check(arg_summary))
                {
                    PyErr_SetString(PyExc_TypeError, "summary must be None or an arkimet.Summary object");
                    return nullptr;
                } else {
                    summary = ((arkipy_Summary*)arg_summary)->summary;
                }
            }

            if (summary)
            {
                self->ds->query_summary(matcher, *summary);
                Py_INCREF(arg_summary);
                return (PyObject*)arg_summary;
            }
            else
            {
                py_unique_ptr<arkipy_Summary> res(summary_create());
                self->ds->query_summary(matcher, *res->summary);
                return (PyObject*)res.release();
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_bytes : public MethKwargs<query_bytes, arkipy_DatasetReader>
{
    constexpr static const char* name = "query_bytes";
    constexpr static const char* signature = "file: Union[int, BinaryIO], matcher: Union[arki.Matcher, str]=None, with_data: bool=False, sort: str=None, data_start_hook: Callable[[], None]=None, postprocess: str=None, metadata_report: str=None, summary_report: str=None";
    constexpr static const char* returns = "None";
    constexpr static const char* summary = "query a dataset, piping results to a file";
    constexpr static const char* doc = R"(
Arguments:
  file: the output file. The file needs to be either an integer file or
        socket handle, or a file-like object with a fileno() method
        that returns an integer handle.
  matcher: the matcher string to filter data to return.
  with_data: if True, also load data together with the metadata.
  sort: string with the desired sort order of results.
  data_start_hook: function called before sending the data to the file
  postprocess: name of a postprocessor to use to filter data server-side
  metadata_report: name of the server-side report function to run on results metadata
  summary_report: name of the server-side report function to run on results summary
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", "matcher", "with_data", "sort", "data_start_hook", "postprocess", "metadata_report", "summary_report", NULL };
        PyObject* arg_file = Py_None;
        PyObject* arg_matcher = Py_None;
        PyObject* arg_with_data = Py_None;
        PyObject* arg_sort = Py_None;
        PyObject* arg_data_start_hook = Py_None;
        PyObject* arg_postprocess = Py_None;
        PyObject* arg_metadata_report = Py_None;
        PyObject* arg_summary_report = Py_None;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|OOOOOOO", const_cast<char**>(kwlist), &arg_file, &arg_matcher, &arg_with_data, &arg_sort, &arg_data_start_hook, &arg_postprocess, &arg_metadata_report, &arg_summary_report))
            return nullptr;

        try {
            BinaryOutputFile out(arg_file);

            arki::Matcher matcher = matcher_from_python(arg_matcher);
            bool with_data = false;
            if (arg_with_data != Py_None)
            {
                int istrue = PyObject_IsTrue(arg_with_data);
                if (istrue == -1) return nullptr;
                with_data = istrue == 1;
            }
            string sort;
            if (arg_sort != Py_None)
                sort = string_from_python(arg_sort);
            string postprocess;
            if (arg_postprocess != Py_None)
                postprocess = string_from_python(arg_postprocess);
            string metadata_report;
            if (arg_metadata_report != Py_None)
                metadata_report = string_from_python(arg_metadata_report);
            string summary_report;
            if (arg_summary_report != Py_None)
                summary_report = string_from_python(arg_summary_report);
            if (arg_data_start_hook != Py_None && !PyCallable_Check(arg_data_start_hook))
            {
                PyErr_SetString(PyExc_TypeError, "data_start_hoook must be None or a callable object");
                return nullptr;
            }

            arki::dataset::ByteQuery query;
            query.with_data = with_data;
            if (!sort.empty()) query.sorter = sort::Compare::parse(sort);
            if (!metadata_report.empty())
                query.setRepMetadata(matcher, metadata_report);
            else if (!summary_report.empty())
                query.setRepSummary(matcher, summary_report);
            else if (!postprocess.empty())
                query.setPostprocess(matcher, postprocess);
            else
                query.setData(matcher);

            pyo_unique_ptr data_start_hook_args;
            if (arg_data_start_hook != Py_None)
            {
                data_start_hook_args = pyo_unique_ptr(Py_BuildValue("()"));
                if (!data_start_hook_args) return nullptr;

                query.data_start_hook = [&](NamedFileDescriptor& fd) {
                    // call arg_data_start_hook
                    pyo_unique_ptr res(PyObject_CallObject(arg_data_start_hook, data_start_hook_args));
                    if (!res) throw PythonException();
                };
            }

            if (out.fd)
                self->ds->query_bytes(query, *out.fd);
            else
                self->ds->query_bytes(query, *out.abstract);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetReaderDef : public Type<DatasetReaderDef, arkipy_DatasetReader>
{
    constexpr static const char* name = "Reader";
    constexpr static const char* qual_name = "arkimet.dataset.Reader";
    constexpr static const char* doc = R"(
Read functions for an arkimet dataset.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<query_data, query_summary, query_bytes> methods;

    static void _dealloc(Impl* self)
    {
        self->ds.~shared_ptr<arki::dataset::Reader>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Reader(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Reader(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
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
                cfg = arki::dataset::Reader::read_config(from_python<std::string>(py_cfg));
            else
                cfg = section_from_python(py_cfg);

            new (&(self->ds)) shared_ptr<arki::dataset::Reader>(arki::dataset::Reader::create(cfg));
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetReaderDef* reader_def = nullptr;


/*
 * dataset.Writer
 */

struct acquire : public MethKwargs<acquire, arkipy_DatasetWriter>
{
    constexpr static const char* name = "acquire";
    constexpr static const char* signature = "md: arki.Metadata, replace: str=None, drop_cached_data_on_commit: bool=False";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "Acquire the given metadata item (and related data) in this dataset";
    constexpr static const char* doc = R"(
After acquiring the data successfully, the data can be retrieved from
the dataset.  Also, information such as the dataset name and the id of
the data in the dataset are added to the Metadata object.

If the import failed, a subclass of arki.dataset.ImportError is raised.
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "md", "replace", "drop_cached_data_on_commit", NULL };
        PyObject* arg_md = Py_None;
        const char* arg_replace = nullptr;
        Py_ssize_t arg_replace_len;
        int drop_cached_data_on_commit = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!|s#p", const_cast<char**>(kwlist),
                arkipy_Metadata_Type, &arg_md,
                &arg_replace, &arg_replace_len, &drop_cached_data_on_commit))
            return nullptr;

        try {
            arki::dataset::AcquireConfig cfg;
            if (arg_replace)
            {
                std::string replace(arg_replace, arg_replace_len);
                if (replace == "default")
                    cfg.replace = arki::dataset::REPLACE_DEFAULT;
                else if (replace == "never")
                    cfg.replace = arki::dataset::REPLACE_NEVER;
                else if (replace == "always")
                    cfg.replace = arki::dataset::REPLACE_ALWAYS;
                else if (replace == "higher_usn")
                    cfg.replace = arki::dataset::REPLACE_HIGHER_USN;
                else
                {
                    PyErr_SetString(PyExc_ValueError, "replace argument must be 'default', 'never', 'always', or 'higher_usn'");
                    return nullptr;
                }
            }

            cfg.drop_cached_data_on_commit = drop_cached_data_on_commit;

            arki::dataset::WriterAcquireResult res;
            {
                ReleaseGIL gil;
                res = self->ds->acquire(*((arkipy_Metadata*)arg_md)->md, cfg);
            }

            switch (res)
            {
                case arki::dataset::ACQ_OK:
                    Py_RETURN_NONE;
                case arki::dataset::ACQ_ERROR_DUPLICATE:
                    PyErr_SetString(arkipy_ImportDuplicateError, "data already exists in the dataset");
                    return nullptr;
                case arki::dataset::ACQ_ERROR:
                    PyErr_SetString(arkipy_ImportFailedError, "import failed");
                    return nullptr;
                default:
                    PyErr_SetString(arkipy_ImportError, "unexpected result from dataset import");
                    return nullptr;
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct flush : public MethNoargs<flush, arkipy_DatasetWriter>
{
    constexpr static const char* name = "flush";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "Flush pending changes to disk";

    static PyObject* run(Impl* self)
    {
        try {
            self->ds->flush();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetWriterDef : public Type<DatasetWriterDef, arkipy_DatasetWriter>
{
    constexpr static const char* name = "Writer";
    constexpr static const char* qual_name = "arkimet.dataset.Writer";
    constexpr static const char* doc = R"(
Write functions for an arkimet dataset.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<acquire, flush> methods;

    static void _dealloc(Impl* self)
    {
        self->ds.~shared_ptr<arki::dataset::Writer>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Writer(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Writer(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
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
                cfg = arki::dataset::Reader::read_config(from_python<std::string>(py_cfg));
            else
                cfg = section_from_python(py_cfg);

            new (&(self->ds)) std::shared_ptr<arki::dataset::Writer>(arki::dataset::Writer::create(cfg));
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetWriterDef* writer_def = nullptr;


/*
 * dataset.Checker
 */

static arki::dataset::CheckerConfig get_checker_config(PyObject* args, PyObject* kw)
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
            auto cfg = get_checker_config(args, kw);

            {
                ReleaseGIL gil;
                self->ds->repack(cfg);
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
            auto cfg = get_checker_config(args, kw);

            {
                ReleaseGIL gil;
                self->ds->check(cfg);
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
            auto cfg = get_checker_config(args, kw);

            arki::dataset::segmented::Checker* checker = dynamic_cast<arki::dataset::segmented::Checker*>(self->ds.get());
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
    Methods<repack, check, segment_state> methods;

    static void _dealloc(Impl* self)
    {
        self->ds.~shared_ptr<arki::dataset::Checker>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Checker(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Checker(%s, %s)", self->ds->type().c_str(), self->ds->name().c_str());
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
                cfg = arki::dataset::Reader::read_config(from_python<std::string>(py_cfg));
            else
                cfg = section_from_python(py_cfg);

            new (&(self->ds)) std::shared_ptr<arki::dataset::Checker>(arki::dataset::Checker::create(cfg));
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetCheckerDef* checker_def = nullptr;


/*
 * dataset.SessionTimeOverride
 */

struct __enter__ : public MethNoargs<__enter__, arkipy_DatasetSessionTimeOverride>
{
    constexpr static const char* name = "__enter__";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "";

    static PyObject* run(Impl* self)
    {
        try {
            Py_INCREF(self);
            return (PyObject*)self;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct __exit__ : public MethKwargs<__exit__, arkipy_DatasetSessionTimeOverride>
{
    constexpr static const char* name = "__exit__";
    constexpr static const char* signature = "ext_type, ext_val, ext_tb";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "exc_type", "exc_val", "exc_tb", nullptr };
        PyObject* exc_type = nullptr;
        PyObject* exc_val = nullptr;
        PyObject* exc_tb = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "OOO", const_cast<char**>(kwlist),
                &exc_type, &exc_val, &exc_tb))
            return nullptr;

        try {
            delete self->o;
            self->o = nullptr;
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetSessionTimeOverrideDef : public Type<DatasetSessionTimeOverrideDef, arkipy_DatasetSessionTimeOverride>
{
    constexpr static const char* name = "SessionTimeOverride";
    constexpr static const char* qual_name = "arkimet.dataset.SessionTimeOverride";
    constexpr static const char* doc = R"(
Write functions for an arkimet dataset.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<__enter__, __exit__> methods;

    static void _dealloc(Impl* self)
    {
        delete self->o;
        self->o = nullptr;
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.SessionTimeOverride()");
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.SessionTimeOverride()");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "time", nullptr };
        unsigned long long arg_time = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "K", const_cast<char**>(kwlist), &arg_time))
            return -1;

        try {
            self->o = new arki::dataset::SessionTimeOverride(arki::dataset::SessionTime::local_override(arg_time));
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetSessionTimeOverrideDef* session_time_override_def = nullptr;

/*
 * dataset module functions
 */

struct read_config : public MethKwargs<read_config, PyObject>
{
    constexpr static const char* name = "read_config";
    constexpr static const char* signature = "pathname: str";
    constexpr static const char* returns = "arki.cfg.Section";
    constexpr static const char* summary = "Read the configuration of a dataset at the given path or URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "pathname", nullptr };
        const char* pathname;
        Py_ssize_t pathname_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &pathname, &pathname_len))
            return nullptr;

        try {
            auto section = arki::dataset::Reader::read_config(std::string(pathname, pathname_len));
            return cfg_section(std::move(section));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct read_configs : public MethKwargs<read_configs, PyObject>
{
    constexpr static const char* name = "read_configs";
    constexpr static const char* signature = "pathname: str";
    constexpr static const char* returns = "arki.cfg.Sections";
    constexpr static const char* summary = "Read the merged dataset configuration at the given path or URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "pathname", nullptr };
        const char* pathname;
        Py_ssize_t pathname_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &pathname, &pathname_len))
            return nullptr;

        try {
            auto sections = arki::dataset::Reader::read_configs(std::string(pathname, pathname_len));
            return cfg_sections(std::move(sections));
        } ARKI_CATCH_RETURN_PYO
    }
};


Methods<read_config, read_configs> dataset_methods;


/*
 * dataset.http module functions
 */

struct load_cfg_sections : public MethKwargs<load_cfg_sections, PyObject>
{
    constexpr static const char* name = "load_cfg_sections";
    constexpr static const char* signature = "url: str";
    constexpr static const char* returns = "arki.cfg.Sections";
    constexpr static const char* summary = "Read the configuration of the datasets at the given URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "url", nullptr };
        const char* url;
        Py_ssize_t url_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &url, &url_len))
            return nullptr;

        try {
            auto sections = arki::dataset::http::Reader::load_cfg_sections(std::string(url, url_len));
            return cfg_sections(std::move(sections));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct get_alias_database : public MethKwargs<get_alias_database, PyObject>
{
    constexpr static const char* name = "get_alias_database";
    constexpr static const char* signature = "url: str";
    constexpr static const char* returns = "arki.cfg.Sections";
    constexpr static const char* summary = "Read the alias database for the server at the given URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "url", nullptr };
        const char* url;
        Py_ssize_t url_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &url, &url_len))
            return nullptr;

        try {
            auto sections = arki::dataset::http::Reader::getAliasDatabase(std::string(url, url_len));
            return cfg_sections(std::move(sections));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct expand_remote_query : public MethKwargs<expand_remote_query, PyObject>
{
    constexpr static const char* name = "expand_remote_query";
    constexpr static const char* signature = "remotes: arkimet.cfg.Sections, query: str";
    constexpr static const char* returns = "str";
    constexpr static const char* summary = "Expand aliases on the query for all remote datasets given.";
    constexpr static const char* doc = "An exception is raised if some remotes have conflicting aliases definition.";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "remotes", "query", nullptr };
        PyObject* remotes = nullptr;
        const char* query;
        Py_ssize_t query_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "Os#", const_cast<char**>(kwlist), &remotes, &query, &query_len))
            return nullptr;

        try {
            std::string expanded = arki::dataset::http::Reader::expand_remote_query(
                    sections_from_python(remotes), std::string(query, query_len));
            return to_python(expanded);
        } ARKI_CATCH_RETURN_PYO
    }
};



Methods<load_cfg_sections, get_alias_database, expand_remote_query> http_methods;

}

extern "C" {

static PyModuleDef dataset_module = {
    PyModuleDef_HEAD_INIT,
    "dataset",         /* m_name */
    "Arkimet dataset functions",  /* m_doc */
    -1,             /* m_size */
    dataset_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

static PyModuleDef http_module = {
    PyModuleDef_HEAD_INIT,
    "http",         /* m_name */
    "Arkimet http dataset functions",  /* m_doc */
    -1,             /* m_size */
    http_methods.as_py(),    /* m_methods */
    nullptr,           /* m_slots */
    nullptr,           /* m_traverse */
    nullptr,           /* m_clear */
    nullptr,           /* m_free */
};

}

namespace arki {
namespace python {

arkipy_DatasetReader* dataset_reader_create()
{
    return (arkipy_DatasetReader*)PyObject_CallObject((PyObject*)arkipy_DatasetReader_Type, NULL);
}

arkipy_DatasetReader* dataset_reader_create(std::shared_ptr<arki::dataset::Reader> ds)
{
    arkipy_DatasetReader* result = PyObject_New(arkipy_DatasetReader, arkipy_DatasetReader_Type);
    if (!result) return nullptr;
    new (&(result->ds)) std::shared_ptr<arki::dataset::Reader>(ds);
    return result;
}

void register_dataset(PyObject* m)
{
    pyo_unique_ptr http = throw_ifnull(PyModule_Create(&http_module));

    pyo_unique_ptr dataset = throw_ifnull(PyModule_Create(&dataset_module));

    arkipy_ImportError = throw_ifnull(PyErr_NewExceptionWithDoc(
            "arkimet.dataset.ImportError",
            "Base class for dataset import errors",
            PyExc_RuntimeError, nullptr));

    arkipy_ImportDuplicateError = throw_ifnull(PyErr_NewExceptionWithDoc(
            "arkimet.dataset.ImportDuplicateError",
            "The item to import already exists on the dataset",
            arkipy_ImportError, nullptr));

    arkipy_ImportFailedError = throw_ifnull(PyErr_NewExceptionWithDoc(
            "arkimet.dataset.ImportFailedError",
            "The import process failed on this metadata",
            arkipy_ImportError, nullptr));

    Py_INCREF(arkipy_ImportError);
    if (PyModule_AddObject(dataset, "ImportError", arkipy_ImportError) == -1)
    {
        Py_DECREF(arkipy_ImportError);
        throw PythonException();
    }

    Py_INCREF(arkipy_ImportDuplicateError);
    if (PyModule_AddObject(dataset, "ImportDuplicateError", arkipy_ImportDuplicateError) == -1)
    {
        Py_DECREF(arkipy_ImportDuplicateError);
        throw PythonException();
    }

    Py_INCREF(arkipy_ImportFailedError);
    if (PyModule_AddObject(dataset, "ImportFailedError", arkipy_ImportFailedError) == -1)
    {
        Py_DECREF(arkipy_ImportFailedError);
        throw PythonException();
    }


    reader_def = new DatasetReaderDef;
    reader_def->define(arkipy_DatasetReader_Type, dataset);

    writer_def = new DatasetWriterDef;
    writer_def->define(arkipy_DatasetWriter_Type, dataset);

    checker_def = new DatasetCheckerDef;
    checker_def->define(arkipy_DatasetChecker_Type, dataset);

    session_time_override_def = new DatasetSessionTimeOverrideDef;
    session_time_override_def->define(arkipy_DatasetSessionTimeOverride_Type, dataset);

    if (PyModule_AddObject(dataset, "http", http.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(m, "dataset", dataset.release()) == -1)
        throw PythonException();
}

}
}
