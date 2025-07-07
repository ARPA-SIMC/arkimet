#define PY_SSIZE_T_CLEAN
#include "python/dataset/writer.h"
#include "arki/core/time.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "python/cfg.h"
#include "python/common.h"
#include "python/metadata.h"
#include "python/utils/methods.h"
#include "python/utils/type.h"
#include "python/utils/values.h"
#include <Python.h>
#include <ctime>
#include <vector>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::python;

extern "C" {

PyTypeObject* arkipy_DatasetWriter_Type = nullptr;

PyObject* arkipy_ImportError          = nullptr;
PyObject* arkipy_ImportDuplicateError = nullptr;
PyObject* arkipy_ImportFailedError    = nullptr;
}

namespace {

/*
 * dataset.Writer
 */

/// Create an AcquireConfig with acquire* method arguments
arki::dataset::AcquireConfig acquire_config(const char* arg_replace,
                                            int arg_replace_len,
                                            bool drop_cached_data_on_commit)
{
    arki::dataset::AcquireConfig cfg;
    if (arg_replace)
    {
        std::string replace(arg_replace, arg_replace_len);
        if (replace == "default")
            cfg.replace = arki::ReplaceStrategy::DEFAULT;
        else if (replace == "never")
            cfg.replace = arki::ReplaceStrategy::NEVER;
        else if (replace == "always")
            cfg.replace = arki::ReplaceStrategy::ALWAYS;
        else if (replace == "higher_usn")
            cfg.replace = arki::ReplaceStrategy::HIGHER_USN;
        else
        {
            PyErr_SetString(PyExc_ValueError,
                            "replace argument must be 'default', 'never', "
                            "'always', or 'higher_usn'");
            throw PythonException();
        }
    }

    cfg.drop_cached_data_on_commit = drop_cached_data_on_commit;

    return cfg;
}

struct acquire : public MethKwargs<acquire, arkipy_DatasetWriter>
{
    constexpr static const char* name = "acquire";
    constexpr static const char* signature =
        "md: arki.Metadata, replace: str=None, drop_cached_data_on_commit: "
        "bool=False";
    constexpr static const char* returns = "";
    constexpr static const char* summary =
        "Acquire the given metadata item (and related data) in this dataset";
    constexpr static const char* doc = R"(
After acquiring the data successfully, the data can be retrieved from
the dataset.  Also, information such as the dataset name and the id of
the data in the dataset are added to the Metadata object.

If the import failed, a subclass of arki.dataset.ImportError is raised.
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"md", "replace",
                                       "drop_cached_data_on_commit", NULL};
        PyObject* arg_md            = Py_None;
        const char* arg_replace     = nullptr;
        Py_ssize_t arg_replace_len;
        int drop_cached_data_on_commit = 0;
        if (!PyArg_ParseTupleAndKeywords(
                args, kw, "O!|s#p", const_cast<char**>(kwlist),
                arkipy_Metadata_Type, &arg_md, &arg_replace, &arg_replace_len,
                &drop_cached_data_on_commit))
            return nullptr;

        try
        {
            auto cfg = acquire_config(arg_replace, arg_replace_len,
                                      drop_cached_data_on_commit);

            metadata::InboundBatch batch;
            batch.add(((arkipy_Metadata*)arg_md)->md);
            {
                ReleaseGIL gil;
                self->ptr->acquire_batch(batch, cfg);
            }

            switch (batch[0]->result)
            {
                case metadata::Inbound::Result::OK: Py_RETURN_NONE;
                case metadata::Inbound::Result::DUPLICATE:
                    PyErr_SetString(arkipy_ImportDuplicateError,
                                    "data already exists in the dataset");
                    return nullptr;
                case metadata::Inbound::Result::ERROR:
                    PyErr_SetString(arkipy_ImportFailedError, "import failed");
                    return nullptr;
                default:
                    PyErr_SetString(arkipy_ImportError,
                                    "unexpected result from dataset import");
                    return nullptr;
            }
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct acquire_batch : public MethKwargs<acquire_batch, arkipy_DatasetWriter>
{
    constexpr static const char* name = "acquire_batch";
    constexpr static const char* signature =
        "md: Iterable[arkimet.Metadata], replace: str=None, "
        "drop_cached_data_on_commit: bool=False";
    constexpr static const char* returns = "Tuple[str]";
    constexpr static const char* summary =
        "Acquire the given metadata items (and related data) in this dataset";
    constexpr static const char* doc = R"(
After acquiring the data successfully, the data can be retrieved from
the dataset.  Also, information such as the dataset name and the id of
the data in the dataset are added to the Metadata objects.

No exception is raised in case of import failures. The function returns a tuple
with the same length and the input sequence of metadata, and a string
representing the outcome: "ok", "duplicate", or "error".
)";

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"mds", "replace",
                                       "drop_cached_data_on_commit", NULL};
        PyObject* arg_mds           = Py_None;
        const char* arg_replace     = nullptr;
        Py_ssize_t arg_replace_len;
        int drop_cached_data_on_commit = 0;
        if (!PyArg_ParseTupleAndKeywords(
                args, kw, "O|s#p", const_cast<char**>(kwlist), &arg_mds,
                &arg_replace, &arg_replace_len, &drop_cached_data_on_commit))
            return nullptr;

        try
        {
            auto cfg = acquire_config(arg_replace, arg_replace_len,
                                      drop_cached_data_on_commit);

            metadata::InboundBatch batch;

            // Iterate the input metadata and fill the batch
            pyo_unique_ptr iterator(throw_ifnull(PyObject_GetIter(arg_mds)));

            while (true)
            {
                py_unique_ptr<arkipy_Metadata> item(
                    (arkipy_Metadata*)PyIter_Next(iterator));
                if (!item)
                {
                    if (PyErr_Occurred())
                        throw PythonException();
                    else
                        break;
                }

                batch.emplace_back(
                    std::make_shared<metadata::Inbound>(item->md));
            }

            {
                ReleaseGIL gil;
                self->ptr->acquire_batch(batch, cfg);
            }

            pyo_unique_ptr res(PyTuple_New(batch.size()));
            unsigned offset = 0;
            for (const auto& el : batch)
            {
                pyo_unique_ptr val;
                switch (el->result)
                {
                    case metadata::Inbound::Result::OK:
                        val = to_python("ok");
                        break;
                    case metadata::Inbound::Result::DUPLICATE:
                        val = to_python("duplicate");
                        break;
                    case metadata::Inbound::Result::ERROR:
                        val = to_python("error");
                        break;
                    default:
                        PyErr_SetString(
                            arkipy_ImportError,
                            "unexpected result from dataset import");
                        throw PythonException();
                }

                PyTuple_SET_ITEM(res.get(), offset, val.release());
                ++offset;
            }

            return res.release();
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct flush : public MethNoargs<flush, arkipy_DatasetWriter>
{
    constexpr static const char* name      = "flush";
    constexpr static const char* signature = "";
    constexpr static const char* returns   = "";
    constexpr static const char* summary   = "Flush pending changes to disk";

    static PyObject* run(Impl* self)
    {
        try
        {
            self->ptr->flush();
            Py_RETURN_NONE;
        }
        ARKI_CATCH_RETURN_PYO
    }
};

struct DatasetWriterDef : public Type<DatasetWriterDef, arkipy_DatasetWriter>
{
    constexpr static const char* name      = "Writer";
    constexpr static const char* qual_name = "arkimet.dataset.Writer";
    constexpr static const char* doc       = R"(
Write functions for an arkimet dataset.

TODO: document

Examples::

    TODO: add examples
)";
    GetSetters<> getsetters;
    Methods<MethGenericEnter<Impl>, MethGenericExit<Impl>, acquire,
            acquire_batch, flush>
        methods;

    static void _dealloc(Impl* self)
    {
        self->ptr.~shared_ptr<arki::dataset::Writer>();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject* _str(Impl* self)
    {
        if (self->ptr.get())
            return PyUnicode_FromFormat("dataset.Writer(%s, %s)",
                                        self->ptr->type().c_str(),
                                        self->ptr->name().c_str());
        else
            return to_python("dataset.Writer(<out of scope>)");
    }

    static PyObject* _repr(Impl* self)
    {
        if (self->ptr.get())
            return PyUnicode_FromFormat("dataset.Writer(%s, %s)",
                                        self->ptr->type().c_str(),
                                        self->ptr->name().c_str());
        else
            return to_python("dataset.Writer(<out of scope>)");
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = {"cfg", nullptr};
        PyObject* py_cfg            = Py_None;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O",
                                         const_cast<char**>(kwlist), &py_cfg))
            return -1;

        try
        {
            std::shared_ptr<core::cfg::Section> cfg;

            if (PyUnicode_Check(py_cfg))
                cfg = arki::dataset::Session::read_config(
                    from_python<std::string>(py_cfg));
            else
                cfg = section_from_python(py_cfg);

            if (PyErr_WarnEx(
                    PyExc_DeprecationWarning,
                    "Use arki.dataset.Session().dataset_writer(cfg=cfg) "
                    "instead of arki.dataset.Writer(cfg)",
                    1))
                return -1;

            auto session = std::make_shared<arki::dataset::Session>();
            new (&(self->ptr)) std::shared_ptr<arki::dataset::Writer>(
                session->dataset(*cfg)->create_writer());
            return 0;
        }
        ARKI_CATCH_RETURN_INT;
    }
};

DatasetWriterDef* writer_def = nullptr;

} // namespace

namespace arki {
namespace python {

arkipy_DatasetWriter*
dataset_writer_create(std::shared_ptr<arki::dataset::Writer> ds)
{
    arkipy_DatasetWriter* result =
        PyObject_New(arkipy_DatasetWriter, arkipy_DatasetWriter_Type);
    if (!result)
        return nullptr;
    new (&(result->ptr)) std::shared_ptr<arki::dataset::Writer>(ds);
    return result;
}

void register_dataset_writer(PyObject* module)
{
    arkipy_ImportError = throw_ifnull(PyErr_NewExceptionWithDoc(
        "arkimet.dataset.ImportError", "Base class for dataset import errors",
        PyExc_RuntimeError, nullptr));

    arkipy_ImportDuplicateError = throw_ifnull(PyErr_NewExceptionWithDoc(
        "arkimet.dataset.ImportDuplicateError",
        "The item to import already exists on the dataset", arkipy_ImportError,
        nullptr));

    arkipy_ImportFailedError = throw_ifnull(
        PyErr_NewExceptionWithDoc("arkimet.dataset.ImportFailedError",
                                  "The import process failed on this metadata",
                                  arkipy_ImportError, nullptr));

    Py_INCREF(arkipy_ImportError);
    if (PyModule_AddObject(module, "ImportError", arkipy_ImportError) == -1)
    {
        Py_DECREF(arkipy_ImportError);
        throw PythonException();
    }

    Py_INCREF(arkipy_ImportDuplicateError);
    if (PyModule_AddObject(module, "ImportDuplicateError",
                           arkipy_ImportDuplicateError) == -1)
    {
        Py_DECREF(arkipy_ImportDuplicateError);
        throw PythonException();
    }

    Py_INCREF(arkipy_ImportFailedError);
    if (PyModule_AddObject(module, "ImportFailedError",
                           arkipy_ImportFailedError) == -1)
    {
        Py_DECREF(arkipy_ImportFailedError);
        throw PythonException();
    }

    writer_def = new DatasetWriterDef;
    writer_def->define(arkipy_DatasetWriter_Type, module);
}

} // namespace python
} // namespace arki
