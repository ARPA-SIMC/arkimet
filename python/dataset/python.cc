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
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/core/time.h"

namespace arki {
namespace python {
namespace dataset {

struct PyDatasetReader : public arki::dataset::DatasetAccess<arki::dataset::Dataset, arki::dataset::Reader>
{
    std::string m_type;

    PyObject* o;
    PyObject* meth_query_data = nullptr;
    PyObject* meth_query_summary = nullptr;

    PyDatasetReader(std::shared_ptr<arki::dataset::Session> session, PyObject* o)
        : DatasetAccess(std::make_shared<arki::dataset::Dataset>(session)),
          o(o)
    {
        AcquireGIL gil;
        Py_XINCREF(o);
        meth_query_data = throw_ifnull(PyObject_GetAttrString(o, "query_data"));
        meth_query_summary = PyObject_GetAttrString(o, "query_summary");
        if (!meth_query_summary)
            PyErr_Clear();

        pyo_unique_ptr type(PyObject_GetAttrString(o, "type"));
        if (!type)
            PyErr_Clear();
        else
            m_type = from_python<std::string>(type);

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

    bool impl_query_data(const arki::query::Data& q, arki::metadata_dest_func dest) override
    {
        arki::query::TrackProgress track(q.progress);
        dest = track.wrap(dest);

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
            return track.done(sorter->flush());
        else
        {
            if (res == Py_None)
                return track.done(true);
            return track.done(from_python<bool>(res));
        }
    }

    void impl_query_summary(const Matcher& matcher, Summary& summary) override
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
            arki::dataset::Reader::impl_query_summary(matcher, summary);
        }
    }

    core::Interval get_stored_time_interval() override
    {
        throw std::runtime_error("python::Reader::get_stored_time_interval not yet implemented");
    }
};

std::shared_ptr<arki::dataset::Reader> create_reader(std::shared_ptr<arki::dataset::Session> session, PyObject* o)
{
    return std::make_shared<PyDatasetReader>(session, o);
}

std::shared_ptr<arki::dataset::Writer> create_writer(std::shared_ptr<arki::dataset::Session> session, PyObject* o)
{
    PyErr_SetString(PyExc_NotImplementedError, "creating python dataset writer not implemented yet");
    throw PythonException();
}

std::shared_ptr<arki::dataset::Checker> create_checker(std::shared_ptr<arki::dataset::Session> session, PyObject* o)
{
    PyErr_SetString(PyExc_NotImplementedError, "creating python dataset checker not implemented yet");
    throw PythonException();
}

}
}
}
