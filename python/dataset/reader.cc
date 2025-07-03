#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "python/dataset/reader.h"
#include "python/common.h"
#include "python/metadata.h"
#include "python/summary.h"
#include "python/cfg.h"
#include "python/files.h"
#include "python/matcher.h"
#include "python/utils/values.h"
#include "python/utils/methods.h"
#include "python/utils/type.h"
#include "arki/core/file.h"
#include "arki/core/time.h"
#include "arki/stream.h"
#include "arki/metadata/sort.h"
#include "arki/dataset.h"
#include "arki/query.h"
#include "arki/dataset/session.h"
#include "progress.h"
#include <ctime>
#include <vector>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetReader_Type = nullptr;

}

namespace {

/*
 * dataset.Reader
 */

struct query_data : public MethKwargs<query_data, arkipy_DatasetReader>
{
    constexpr static const char* name = "query_data";
    constexpr static const char* signature = "matcher: Union[arki.Matcher, str]=None, with_data: bool=False, sort: str=None, on_metadata: Callable[[metadata], Optional[bool]]=None";
    constexpr static const char* returns = "Union[None, List[arki.Metadata]]";
    constexpr static const char* summary = "query a dataset, processing the resulting metadata one by one";
    constexpr static const char* doc = R"(
:arg matcher: the matcher string to filter data to return.
:arg with_data: if True, also load data together with the metadata.
:arg sort: string with the desired sort order of results.
:arg on_metadata: a function called on each metadata, with the Metadata
                  object as its only argument. Return None or True to
                  continue processing results, False to stop.
:arg progress: an object with 3 methods: ``start(expected_count: int=0, expected_bytes: int=0)``,
               ``update(count: int, bytes: int)``, and ``done(total_count: int, total_bytes: int)``,
               to call for progress updates.
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "matcher", "with_data", "sort", "on_metadata", "progress", nullptr };
        PyObject* arg_matcher = Py_None;
        PyObject* arg_with_data = Py_None;
        PyObject* arg_sort = Py_None;
        PyObject* arg_on_metadata = Py_None;
        PyObject* arg_progress = Py_None;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "|OOOOO", const_cast<char**>(kwlist),
                    &arg_matcher, &arg_with_data, &arg_sort, &arg_on_metadata, &arg_progress))
            return nullptr;

        try {
            arki::query::Data query(matcher_from_python(self->ptr->dataset().session, arg_matcher));
            if (arg_with_data != Py_None)
                query.with_data = from_python<bool>(arg_with_data);
            string sort;
            if (arg_sort != Py_None)
                sort = string_from_python(arg_sort);
            if (!sort.empty()) query.sorter = metadata::sort::Compare::parse(sort);
            if (arg_progress == Py_None)
                query.progress = std::make_shared<python::dataset::PythonProgress>();
            else
                query.progress = std::make_shared<python::dataset::PythonProgress>(arg_progress);

            metadata_dest_func dest;
            pyo_unique_ptr res_list;
            if (arg_on_metadata == Py_None)
            {
                res_list.reset(throw_ifnull(PyList_New(0)));

                dest = [&](std::shared_ptr<Metadata> md) {
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
                res = self->ptr->query_data(query, dest);
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
    constexpr static const char* signature = "matcher: Union[arki.Matcher, str]=None, summary: arkimet.Summary=None, progress=None";
    constexpr static const char* returns = "arkimet.Summary";
    constexpr static const char* summary = "query a dataset, returning an arkimet.Summary with the results";
    constexpr static const char* doc = R"(
:arg matcher: the matcher string to filter data to return.
:arg summary: not None, add results to this arkimet.Summary, and return
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
            auto matcher = matcher_from_python(self->ptr->dataset().session, arg_matcher);

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
                self->ptr->query_summary(matcher, *summary);
                Py_INCREF(arg_summary);
                return (PyObject*)arg_summary;
            }
            else
            {
                py_unique_ptr<arkipy_Summary> res(summary_create());
                self->ptr->query_summary(matcher, *res->summary);
                return (PyObject*)res.release();
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_bytes : public MethKwargs<query_bytes, arkipy_DatasetReader>
{
    constexpr static const char* name = "query_bytes";
    constexpr static const char* signature = "matcher: Union[arki.Matcher, str]=None, with_data: bool=False, sort: str=None, data_start_hook: Callable[[], None]=None, postprocess: str=None, metadata_report: str=None, summary_report: str=None, file: Union[int, BinaryIO]=None, progres=None";
    constexpr static const char* returns = "Union[None, bytes]";
    constexpr static const char* summary = "query a dataset, piping results to a file";
    constexpr static const char* doc = R"(
:arg matcher: the matcher string to filter data to return.
:arg with_data: if True, also load data together with the metadata.
:arg sort: string with the desired sort order of results.
:arg data_start_hook: function called before sending the data to the file
:arg postprocess: name of a postprocessor to use to filter data server-side
:arg metadata_report: name of the server-side report function to run on results metadata
:arg summary_report: name of the server-side report function to run on results summary
:arg file: the output file. The file can be a file-like object, or an integer file
           or socket handle. If missing, data is returned in a bytes object
:arg progress: an object with 3 methods: ``start(expected_count: int=0, expected_bytes: int=0)``,
               ``update(count: int, bytes: int)``, and ``done(total_count: int, total_bytes: int)``,
               to call for progress updates.
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "matcher", "with_data", "sort", "data_start_hook", "postprocess", "file", "progress", nullptr };
        PyObject* arg_matcher = Py_None;
        PyObject* arg_with_data = Py_None;
        PyObject* arg_sort = Py_None;
        PyObject* arg_data_start_hook = Py_None;
        PyObject* arg_postprocess = Py_None;
        PyObject* arg_file = Py_None;
        PyObject* arg_progress = Py_None;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "|OOOOOOO", const_cast<char**>(kwlist),
                    &arg_matcher, &arg_with_data, &arg_sort, &arg_data_start_hook, &arg_postprocess, &arg_file, &arg_progress))
            return nullptr;

        try {
            arki::Matcher matcher = matcher_from_python(self->ptr->dataset().session, arg_matcher);
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
            if (arg_data_start_hook != Py_None && !PyCallable_Check(arg_data_start_hook))
            {
                PyErr_SetString(PyExc_TypeError, "data_start_hoook must be None or a callable object");
                return nullptr;
            }

            arki::query::Bytes query;
            query.with_data = with_data;
            if (!postprocess.empty())
                query.setPostprocess(matcher, postprocess);
            else
                query.setData(matcher);

            if (!sort.empty())
                query.sorter = metadata::sort::Compare::parse(sort);

            if (arg_progress == Py_None)
                query.progress = std::make_shared<python::dataset::PythonProgress>();
            else
                query.progress = std::make_shared<python::dataset::PythonProgress>(arg_progress);

            // Call data_start_hook now that we have validated all arguments
            // and we are ready to perform the query
            pyo_unique_ptr data_start_hook_args;
            std::function<arki::stream::SendResult(StreamOutput&)> data_start_callback;
            if (arg_data_start_hook != Py_None)
            {
                data_start_hook_args = pyo_unique_ptr(Py_BuildValue("()"));
                if (!data_start_hook_args) return nullptr;

                pyo_unique_ptr res(PyObject_CallObject(arg_data_start_hook, data_start_hook_args));
                if (!res) throw PythonException();
            }

            if (arg_file && arg_file != Py_None)
            {
                std::unique_ptr<arki::StreamOutput> stream = binaryio_stream_output(arg_file);

                {
                    ReleaseGIL gil;
                    self->ptr->query_bytes(query, *stream);
                }
                Py_RETURN_NONE;
            } else {
                std::vector<uint8_t> buffer;
                auto out = StreamOutput::create(buffer);
                {
                    ReleaseGIL gil;
                    self->ptr->query_bytes(query, *out);
                }
                return to_python(buffer);
            }
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
    Methods<MethGenericEnter<Impl>, MethGenericExit<Impl>, query_data, query_summary, query_bytes> methods;

    static void _dealloc(Impl* self)
    {
        self->ptr.~shared_ptr<arki::dataset::Reader>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        if (self->ptr.get())
            return PyUnicode_FromFormat("dataset.Reader(%s, %s)", self->ptr->type().c_str(), self->ptr->name().c_str());
        else
            return to_python("dataset.Reader(<out of scope>)");
    }

    static PyObject* _repr(Impl* self)
    {
        if (self->ptr.get())
            return PyUnicode_FromFormat("dataset.Reader(%s, %s)", self->ptr->type().c_str(), self->ptr->name().c_str());
        else
            return to_python("dataset.Reader(<out of scope>)");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "cfg", nullptr };
        PyObject* py_cfg = Py_None;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_cfg))
            return -1;

        try {
            std::shared_ptr<core::cfg::Section> cfg;

            if (PyUnicode_Check(py_cfg))
                cfg = arki::dataset::Session::read_config(from_python<std::string>(py_cfg));
            else
                cfg = section_from_python(py_cfg);

            if (PyErr_WarnEx(PyExc_DeprecationWarning, "Use arki.dataset.Session().dataset_reader(cfg=cfg) instead of arki.dataset.Reader(cfg)", 1))
                return -1;

            auto session = std::make_shared<arki::dataset::Session>();
            new (&(self->ptr)) std::shared_ptr<arki::dataset::Reader>(session->dataset(*cfg)->create_reader());
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetReaderDef* reader_def = nullptr;

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
    new (&(result->ptr)) std::shared_ptr<arki::dataset::Reader>(ds);
    return result;
}

void register_dataset_reader(PyObject* module)
{
    reader_def = new DatasetReaderDef;
    reader_def->define(arkipy_DatasetReader_Type, module);
}

}
}

