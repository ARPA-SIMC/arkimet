#ifndef ARKI_PYTHON_DATASET_REPORTER_H
#define ARKI_PYTHON_DATASET_REPORTER_H

#include "arki/dataset/reporter.h"
#include "python/utils/core.h"
#include <sstream>

namespace arki {
namespace python {
namespace dataset {

class ProxyReporter : public arki::dataset::Reporter
{
protected:
    PyObject* o;

public:
    ProxyReporter(PyObject* o)
        : o(o)
    {
        Py_INCREF(o);
    }
    ProxyReporter(const ProxyReporter&) = delete;
    ProxyReporter(ProxyReporter&&) = delete;
    ~ProxyReporter()
    {
        Py_DECREF(o);
    }
    ProxyReporter& operator=(const ProxyReporter&) = delete;
    ProxyReporter& operator=(ProxyReporter&&) = delete;

    void operation_progress(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "operation_progress", "s#s#s#",
                    ds.data(), ds.size(),
                    operation.data(), operation.size(),
                    message.data(), message.size()));
    }
    void operation_manual_intervention(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "operation_manual_intervention", "s#s#s#",
                    ds.data(), ds.size(),
                    operation.data(), operation.size(),
                    message.data(), message.size()));
    }
    void operation_aborted(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "operation_aborted", "s#s#s#",
                    ds.data(), ds.size(),
                    operation.data(), operation.size(),
                    message.data(), message.size()));
    }
    void operation_report(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "operation_report", "s#s#s#",
                    ds.data(), ds.size(),
                    operation.data(), operation.size(),
                    message.data(), message.size()));
    }
    void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_info", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_repack(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_repack", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_archive(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_archive", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_delete(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_delete", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_deindex(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_deindex", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_rescan(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_rescan", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_tar(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_tar", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_compress(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_compress", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_issue51(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_issue51", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
    void segment_manual_intervention(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(
                    o, "segment_manual_intervention", "s#s#s#",
                    ds.data(), ds.size(),
                    relpath.data(), relpath.size(),
                    message.data(), message.size()));
    }
};


class TextIOReporter : public arki::dataset::Reporter
{
protected:
    PyObject* o;

public:
    TextIOReporter(PyObject* o)
        : o(o)
    {
        Py_INCREF(o);
    }
    TextIOReporter(const TextIOReporter&) = delete;
    TextIOReporter(TextIOReporter&&) = delete;
    ~TextIOReporter()
    {
        Py_DECREF(o);
    }
    TextIOReporter& operator=(const TextIOReporter&) = delete;
    TextIOReporter& operator=(TextIOReporter&&) = delete;

    void write(const std::string& str)
    {
        AcquireGIL gil;
        throw_ifnull(PyObject_CallMethod(o, "write", "s#", str.data(), str.size()));
    }

    void operation_progress(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ": " << operation << ": " << message << std::endl;
        write(out.str());
    }
    void operation_manual_intervention(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ": " << operation << " manual intervention required: " << message << std::endl;
        write(out.str());
    }
    void operation_aborted(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ": " << operation << " aborted: " << message << std::endl;
        write(out.str());
    }
    void operation_report(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ": " << operation << " " << message << std::endl;
        write(out.str());
    }
    void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_repack(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_archive(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_delete(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_deindex(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_rescan(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_tar(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_compress(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_issue51(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
    void segment_manual_intervention(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        std::stringstream out;
        out << ds << ":" << relpath << ": " << message << std::endl;
        write(out.str());
    }
};

}
}
}

#endif
