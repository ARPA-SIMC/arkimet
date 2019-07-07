#include <Python.h>
#include "dataset.h"
#include "common.h"
#include "metadata.h"
#include "summary.h"
#include "cfg.h"
#include "utils/values.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/dataset.h"
#include "arki/dataset/http.h"
#include "arki/sort.h"
#include <vector>
#include "config.h"

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
    constexpr static const char* signature = "on_metadata: Callable[[metadata], Optional[bool]], matcher: str=None, with_data: bool=False, sort: str=None";
    constexpr static const char* returns = "None";
    constexpr static const char* summary = "query a dataset, processing the resulting metadata one by one";
    constexpr static const char* doc = R"(
Arguments:
  on_metadata: a function called on each metadata, with the Metadata
               object as its only argument. Return None or True to
               continue processing results, False to stop.
  matcher: the matcher string to filter data to return.
  with_data: if True, also load data together with the metadata.
  sort: string with the desired sort order of results.
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "on_metadata", "matcher", "with_data", "sort", NULL };
        PyObject* arg_on_metadata = Py_None;
        PyObject* arg_matcher = Py_None;
        PyObject* arg_with_data = Py_None;
        PyObject* arg_sort = Py_None;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "O|OOO", const_cast<char**>(kwlist), &arg_on_metadata, &arg_matcher, &arg_with_data, &arg_sort))
            return nullptr;

        if (!PyCallable_Check(arg_on_metadata))
        {
            PyErr_SetString(PyExc_TypeError, "on_metadata must be a callable object");
            return nullptr;
        }

        try {
            std::string str_matcher;
            if (arg_matcher != Py_None)
                str_matcher = string_from_python(arg_matcher);
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

            dataset::DataQuery query;
            query.matcher = Matcher::parse(str_matcher);
            query.with_data = with_data;
            if (!sort.empty()) query.sorter = sort::Compare::parse(sort);

            metadata_dest_func dest = [&](std::unique_ptr<Metadata>&& md) {
                // call arg_on_metadata
                py_unique_ptr<arkipy_Metadata> pymd(metadata_create(move(md)));
                pyo_unique_ptr args(PyTuple_Pack(1, pymd.get()));
                if (!args) throw PythonException();
                pyo_unique_ptr res(PyObject_CallObject(arg_on_metadata, args));
                if (!res) throw PythonException();
                // Continue if the callback returns None or True
                if (res == Py_None) return true;
                int cont = PyObject_IsTrue(res);
                if (cont == -1) throw PythonException();
                return cont == 1;
            };

            self->ds->query_data(query, dest);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_summary : public MethKwargs<query_summary, arkipy_DatasetReader>
{
    constexpr static const char* name = "query_summary";
    constexpr static const char* signature = "matcher: str=None, summary: arkimet.Summary=None";
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
            std::string str_matcher;
            if (arg_matcher != Py_None)
                str_matcher = string_from_python(arg_matcher);

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
                self->ds->query_summary(Matcher::parse(str_matcher), *summary);
                Py_INCREF(arg_summary);
                return (PyObject*)arg_summary;
            }
            else
            {
                py_unique_ptr<arkipy_Summary> res(summary_create());
                self->ds->query_summary(Matcher::parse(str_matcher), *res->summary);
                return (PyObject*)res.release();
            }
        } ARKI_CATCH_RETURN_PYO
    }
};

struct query_bytes : public MethKwargs<query_bytes, arkipy_DatasetReader>
{
    constexpr static const char* name = "query_bytes";
    constexpr static const char* signature = "file: Union[int, BinaryIO], matcher: str=None, with_data: bool=False, sort: str=None, data_start_hook: Callable[[], None]=None, postprocess: str=None, metadata_report: str=None, summary_report: str=None";
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
            int fd = file_get_fileno(arg_file);
            if (fd == -1) return nullptr;
            string fd_name;
            if (object_repr(arg_file, fd_name) == -1) return nullptr;

            string str_matcher;
            if (arg_matcher != Py_None)
                str_matcher = string_from_python(arg_matcher);
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

            Matcher matcher(Matcher::parse(str_matcher));

            dataset::ByteQuery query;
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

            NamedFileDescriptor out(fd, fd_name);
            self->ds->query_bytes(query, out);
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
        delete self->ds;
        self->ds = nullptr;
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
        PyObject* cfg = Py_None;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &cfg))
            return -1;

        try {
            self->ds = dataset::Reader::create(section_from_python(cfg)).release();
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetReaderDef* sections_def = nullptr;


/*
 * dataset module functions
 */

struct read_config : public MethKwargs<read_config, PyObject>
{
    constexpr static const char* name = "read_config";
    constexpr static const char* signature = "pathname: str";
    constexpr static const char* returns = "arki.cfg.Section";
    constexpr static const char* summary = "Read the configuration of the dataset(s) at the given path or URL";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "pathname", nullptr };
        const char* pathname;
        int pathname_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &pathname, &pathname_len))
            return nullptr;

        try {
            auto section = dataset::Reader::read_config(std::string(pathname, pathname_len));
            return cfg_section(std::move(section));
        } ARKI_CATCH_RETURN_PYO
    }
};


Methods<read_config> dataset_methods;


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
        int url_len;

        if (!PyArg_ParseTupleAndKeywords(args, kw, "s#", const_cast<char**>(kwlist), &url, &url_len))
            return nullptr;

        try {
            auto sections = dataset::http::Reader::load_cfg_sections(std::string(url, url_len));
            return cfg_sections(std::move(sections));
        } ARKI_CATCH_RETURN_PYO
    }
};


Methods<load_cfg_sections> http_methods;

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

arkipy_DatasetReader* dataset_reader_create(std::unique_ptr<dataset::Reader>&& ds)
{
    arkipy_DatasetReader* result = PyObject_New(arkipy_DatasetReader, arkipy_DatasetReader_Type);
    if (!result) return nullptr;
    result->ds = ds.release();
    return result;
}

void register_dataset(PyObject* m)
{
    pyo_unique_ptr http = throw_ifnull(PyModule_Create(&http_module));

    pyo_unique_ptr cfg = throw_ifnull(PyModule_Create(&dataset_module));

    sections_def = new DatasetReaderDef;
    sections_def->define(arkipy_DatasetReader_Type, cfg);

    if (PyModule_AddObject(cfg, "http", http.release()) == -1)
        throw PythonException();

    if (PyModule_AddObject(m, "dataset", cfg.release()) == -1)
        throw PythonException();
}

}
}
