#include "python.h"
#include "python/matcher.h"
#include "python/metadata.h"
#include "python/summary.h"
#include "python/dataset.h"
#include "python/utils/values.h"
#include "python/utils/dict.h"
#include "arki/metadata/sort.h"
#include "arki/summary.h"
#include "arki/dataset/impl.h"
#include "arki/dataset/query.h"

namespace arki {
namespace python {
namespace dataset {

struct PyDatasetReader : public arki::dataset::DatasetAccess<arki::dataset::Dataset, arki::dataset::Reader>
{
    std::string m_type;

    PyObject* o;
    PyObject* meth_query_data = nullptr;
    PyObject* meth_query_summary = nullptr;

    PyDatasetReader(const arki::core::cfg::Section& dataset, PyObject* o)
        : DatasetAccess(std::make_shared<arki::dataset::Dataset>(get_dataset_session(), dataset)),
          o(o)
    {
        AcquireGIL gil;
        Py_XINCREF(o);
        meth_query_data = throw_ifnull(PyObject_GetAttrString(o, "query_data"));
        meth_query_summary = PyObject_GetAttrString(o, "query_summary");
        if (!meth_query_summary)
            PyErr_Clear();

        m_type = dataset.value("type");
        if (m_type.empty())
        {
            pyo_unique_ptr type(PyObject_GetAttrString(o, "type"));
            if (!type)
                PyErr_Clear();
            else
                m_type = from_python<std::string>(type);
        }

        if (m_type.empty())
            m_type = o->ob_type->tp_name;
    }

    PyDatasetReader(const PyDatasetReader&) = delete;
    PyDatasetReader(PyDatasetReader&&) = delete;
    PyDatasetReader& operator=(const PyDatasetReader&) = delete;
    PyDatasetReader& operator=(PyDatasetReader&&) = delete;

    ~PyDatasetReader()
    {
        AcquireGIL gil;
        Py_XDECREF(meth_query_summary);
        Py_XDECREF(meth_query_data);
        Py_XDECREF(o);
    }

    std::string type() const override { return m_type; }

    bool query_data(const arki::dataset::DataQuery& q, arki::metadata_dest_func dest) override
    {
        AcquireGIL gil;
        pyo_unique_ptr args(throw_ifnull(PyTuple_New(0)));
        pyo_unique_ptr kwargs(throw_ifnull(PyDict_New()));
        set_dict(kwargs, "matcher", q.matcher);
        set_dict(kwargs, "with_data", q.with_data);

        std::shared_ptr<arki::metadata::sort::Stream> sorter;
        if (q.sorter)
        {
            sorter.reset(new metadata::sort::Stream(*q.sorter, dest));
            dest = [sorter](std::shared_ptr<Metadata> md) { return sorter->add(md); };
        }

        set_dict(kwargs, "on_metadata", dest);

        pyo_unique_ptr res(throw_ifnull(PyObject_Call(meth_query_data, args, kwargs)));

        if (sorter)
            return sorter->flush();
        else
        {
            if (res == Py_None)
                return true;
            return from_python<bool>(res);
        }
    }

    void query_summary(const Matcher& matcher, Summary& summary) override
    {
        if (meth_query_summary)
        {
            AcquireGIL gil;
            pyo_unique_ptr args(throw_ifnull(PyTuple_New(0)));
            pyo_unique_ptr kwargs(throw_ifnull(PyDict_New()));
            pyo_unique_ptr py_summary((PyObject*)summary_create());
            set_dict(kwargs, "matcher", matcher);
            set_dict(kwargs, "summary", py_summary);
            pyo_unique_ptr res(throw_ifnull(PyObject_Call(meth_query_summary, args, kwargs)));
            summary.add(*((arkipy_Summary*)py_summary.get())->summary);
        } else {
            // If the class does not implement query_summary, use the default
            // implementation based on query_data
            arki::dataset::Reader::query_summary(matcher, summary);
        }
    }
};

std::shared_ptr<arki::dataset::Reader> create_reader(const arki::core::cfg::Section& cfg, PyObject* o)
{
    return std::make_shared<PyDatasetReader>(cfg, o);
}

std::shared_ptr<arki::dataset::Writer> create_writer(PyObject* o)
{
    PyErr_SetString(PyExc_NotImplementedError, "creating python dataset writer not implemented yet");
    throw PythonException();
}

std::shared_ptr<arki::dataset::Checker> create_checker(PyObject* o)
{
    PyErr_SetString(PyExc_NotImplementedError, "creating python dataset checker not implemented yet");
    throw PythonException();
}

}
}
}
