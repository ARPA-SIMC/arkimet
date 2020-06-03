#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "python/dataset/writer.h"
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
#include "python/dataset/reader.h"
#include "python/dataset/reporter.h"
#include "python/dataset/progress.h"
#include <ctime>
#include <vector>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetWriter_Type = nullptr;

PyObject* arkipy_ImportError = nullptr;
PyObject* arkipy_ImportDuplicateError = nullptr;
PyObject* arkipy_ImportFailedError = nullptr;

}

namespace {

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
                res = self->ptr->acquire(*((arkipy_Metadata*)arg_md)->md, cfg);
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
            self->ptr->flush();
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
    Methods<MethGenericEnter<Impl>, MethGenericExit<Impl>, acquire, flush> methods;

    static void _dealloc(Impl* self)
    {
        self->ptr.~shared_ptr<arki::dataset::Writer>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Writer(%s, %s)", self->ptr->type().c_str(), self->ptr->name().c_str());
    }

    static PyObject* _repr(Impl* self)
    {
        return PyUnicode_FromFormat("dataset.Writer(%s, %s)", self->ptr->type().c_str(), self->ptr->name().c_str());
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
            new (&(self->ptr)) std::shared_ptr<arki::dataset::Writer>(session->dataset(cfg)->create_writer());
            return 0;
        } ARKI_CATCH_RETURN_INT;
    }
};

DatasetWriterDef* writer_def = nullptr;

}


namespace arki {
namespace python {

void register_dataset_writer(PyObject* module)
{
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
    if (PyModule_AddObject(module, "ImportError", arkipy_ImportError) == -1)
    {
        Py_DECREF(arkipy_ImportError);
        throw PythonException();
    }

    Py_INCREF(arkipy_ImportDuplicateError);
    if (PyModule_AddObject(module, "ImportDuplicateError", arkipy_ImportDuplicateError) == -1)
    {
        Py_DECREF(arkipy_ImportDuplicateError);
        throw PythonException();
    }

    Py_INCREF(arkipy_ImportFailedError);
    if (PyModule_AddObject(module, "ImportFailedError", arkipy_ImportFailedError) == -1)
    {
        Py_DECREF(arkipy_ImportFailedError);
        throw PythonException();
    }

    writer_def = new DatasetWriterDef;
    writer_def->define(arkipy_DatasetWriter_Type, module);
}

}
}

