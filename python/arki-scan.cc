#include "arki-scan.h"
#include "arki/nag.h"
#include "arki-scan/dispatch.h"
#include "arki/dispatcher.h"
#include "arki/utils/string.h"
#include "arki/exceptions.h"
#include "arki/dataset/session.h"
#include "arki/dataset/file.h"
#include "arki/metadata/validator.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include "cfg.h"
#include "cmdline.h"
#include "cmdline/processor.h"
#include "dataset.h"
#include "dataset/session.h"
#include <sysexits.h>

using namespace arki::python;
using namespace arki::utils;

extern "C" {

PyTypeObject* arkipy_ArkiScan_Type = nullptr;

}


namespace {

static std::string moveFile(const std::filesystem::path& source, const std::filesystem::path& targetdir)
{
    auto targetFile = targetdir / str::basename(source);
    if (::rename(source.c_str(), targetFile.c_str()) == -1)
        arki::throw_system_error("cannot move " + source.native() + " to " + targetFile.native());
    return targetFile;
}

static std::string moveFile(const arki::dataset::Reader& ds, const std::filesystem::path& targetdir)
{
    if (const arki::dataset::file::Reader* d = dynamic_cast<const arki::dataset::file::Reader*>(&ds))
        return moveFile(d->dataset().pathname, targetdir);
    else
        return std::string();
}

/**
 * Data source from the path to a file or dataset
 */
struct FileSource
{
    std::shared_ptr<arki::dataset::Dataset> dataset;
    std::shared_ptr<arki::dataset::Reader> reader;

    arki::core::cfg::Section cfg;
    std::string movework;
    std::string moveok;
    std::string moveko;

    FileSource(std::shared_ptr<arki::dataset::Dataset> dataset)
        : dataset(dataset)
    {
    }

    void open()
    {
        if (!movework.empty() && cfg.value("type") == "file")
            cfg.set("path", moveFile(cfg.value("path"), movework));
        reader = dataset->create_reader();
    }

    void close(bool successful)
    {
        if (successful && !moveok.empty())
            moveFile(*reader, moveok);
        else if (!successful && !moveko.empty())
            moveFile(*reader, moveko);
        reader = std::shared_ptr<arki::dataset::Reader>();
    }
};


/**
 * Build a MetadataDispatcher from the given python function args/kwargs
 */
std::unique_ptr<arki_scan::MetadataDispatch> build_dispatcher(
        std::shared_ptr<arki::dataset::Pool> pool, cmdline::DatasetProcessor& processor, PyObject* args, PyObject* kw)
{
    static const char* kwlist[] = {
        "copyok", "copyko",
        "validate",
        "dispatch", "testdispatch",
        "flush_threshold",
        nullptr };

    const char* copyok = nullptr;
    Py_ssize_t copyok_len;
    const char* copyko = nullptr;
    Py_ssize_t copyko_len;
    const char* validate = nullptr;
    Py_ssize_t validate_len;
    PyObject* dispatch = nullptr;
    PyObject* testdispatch = nullptr;
    unsigned long long flush_threshold = 0;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#z#z#OOK", const_cast<char**>(kwlist),
                &copyok, &copyok_len, &copyko, &copyko_len,
                &validate, &validate_len,
                &dispatch, &testdispatch,
                &flush_threshold))
        throw PythonException();

    std::unique_ptr<arki_scan::MetadataDispatch> res(new arki_scan::MetadataDispatch(pool, processor));

    if (testdispatch)
    {
        res->dispatcher = new arki::TestDispatcher(pool_from_python(testdispatch));
    }
    else if (dispatch)
    {
        res->dispatcher = new arki::RealDispatcher(pool_from_python(dispatch));
    }
    else
        throw std::runtime_error("cannot create MetadataDispatch with no --dispatch or --testdispatch information");

    if (validate)
    {
        const arki::metadata::ValidatorRepository& vals = arki::metadata::ValidatorRepository::get();

        // Add validators to dispatcher
        str::Split splitter(std::string(validate, validate_len), ",");
        for (str::Split::const_iterator iname = splitter.begin();
                iname != splitter.end(); ++iname)
        {
            arki::metadata::ValidatorRepository::const_iterator i = vals.find(*iname);
            if (i == vals.end())
                throw std::runtime_error("unknown validator '" + *iname + "'. You can get a list using --validate=list.");
            res->dispatcher->add_validator(*(i->second));
        }
    }

    if (copyok)
        res->dir_copyok = std::string(copyok, copyok_len);
    if (copyko)
        res->dir_copyko = std::string(copyko, copyko_len);

    if (flush_threshold)
        res->flush_threshold = flush_threshold;

    return res;
}

struct set_processor : public MethKwargs<set_processor, arkipy_ArkiScan>
{
    constexpr static const char* name = "set_processor";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "set dataset processor";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try {
            auto processor = build_processor(self->pool, args, kw);
            self->processor = processor.release();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct set_dispatcher : public MethKwargs<set_dispatcher, arkipy_ArkiScan>
{
    constexpr static const char* name = "set_dispatcher";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "set dispatcher";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        try {
            auto dispatcher = build_dispatcher(self->pool, *(self->processor), args, kw);
            self->dispatcher = dispatcher.release();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct scan_file : public MethKwargs<scan_file, arkipy_ArkiScan>
{
    constexpr static const char* name = "scan_file";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-scan --stdin";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", "format", nullptr };

        PyObject* file = nullptr;
        const char* format = nullptr;
        Py_ssize_t format_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "Oz#", const_cast<char**>(kwlist),
                    &file, &format, &format_len))
            return nullptr;

        try {
            bool all_successful;
            {
                BinaryInputFile in(file);
                ReleaseGIL rg;
                all_successful = foreach_file(self->pool->session(), in, std::string(format, format_len), [&](arki::dataset::Reader& reader) {
                    self->processor->process(reader, reader.name());
                });
                self->processor->end();
            }
            if (all_successful)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct scan_sections : public MethKwargs<scan_sections, arkipy_ArkiScan>
{
    constexpr static const char* name = "scan_sections";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-scan";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return nullptr;

        try {
            bool all_successful;
            {
                ReleaseGIL rg;
                all_successful = foreach_sections(self->pool, [&](arki::dataset::Reader& reader) {
                    self->processor->process(reader, reader.name());
                });
                self->processor->end();
            }
            if (all_successful)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
        } ARKI_CATCH_RETURN_PYO
    }
};

static int compute_result(bool all_processed, bool ignore_duplicates, const std::vector<arki_scan::DispatchResults>& results)
{
    bool have_issues = !all_processed;
    for (const auto& res: results)
    {
        if (res.not_imported)
            return EX_CANTCREAT;
        if (res.in_error_dataset)
        {
            have_issues = true;
            continue;
        }
        if (res.duplicates && !ignore_duplicates)
        {
            have_issues = true;
            continue;
        }
        if (!res.successful)
        {
            have_issues = true;
            continue;
        }
    }

    if (have_issues)
        return EX_DATAERR;
    else
        return EX_OK;
}


struct dispatch_file : public MethKwargs<dispatch_file, arkipy_ArkiScan>
{
    constexpr static const char* name = "dispatch_file";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-scan --stdin --dispatch";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "file", "format", "ignore_duplicates", "status", nullptr };

        PyObject* file = nullptr;
        const char* format = nullptr;
        Py_ssize_t format_len;
        int ignore_duplicates = 0;
        int status = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "Oz#|pp", const_cast<char**>(kwlist),
                    &file, &format, &format_len, &ignore_duplicates, &status))
            return nullptr;

        try {
            std::vector<arki_scan::DispatchResults> results;
            bool all_processed = false;
            {
                BinaryInputFile in(file);
                ReleaseGIL rg;

                all_processed = foreach_file(self->pool->session(), in, std::string(format, format_len), [&](arki::dataset::Reader& reader) {
                    auto stats = self->dispatcher->process(reader, reader.name());

                    if (status)
                        arki::nag::warning("%s: %s", stats.name.c_str(), stats.summary().c_str());

                    results.emplace_back(stats);
                });
                self->processor->end();
            }

            return throw_ifnull(PyLong_FromLong(compute_result(all_processed, ignore_duplicates, results)));
        } ARKI_CATCH_RETURN_PYO
    }
};

struct dispatch_sections : public MethKwargs<dispatch_sections, arkipy_ArkiScan>
{
    constexpr static const char* name = "dispatch_sections";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-scan --dispatch";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "moveok", "moveko", "movework", "ignore_duplicates", "status", nullptr };
        const char* moveok = nullptr;
        Py_ssize_t moveok_len;
        const char* moveko = nullptr;
        Py_ssize_t moveko_len;
        const char* movework = nullptr;
        Py_ssize_t movework_len;
        int ignore_duplicates = 0;
        int status = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#z#z#pp", const_cast<char**>(kwlist),
                    &moveok, &moveok_len,
                    &moveko, &moveko_len,
                    &movework, &movework_len,
                    &ignore_duplicates, &status))
            return nullptr;

        try {
            std::vector<arki_scan::DispatchResults> results;
            bool all_processed = false;
            bool had_exceptions = false;

            {
                ReleaseGIL rg;

                // Query all the datasets in sequence
                all_processed = self->pool->foreach_dataset([&](std::shared_ptr<arki::dataset::Dataset> ds) {
                    FileSource source(ds);
                    if (movework) source.movework = std::string(movework, movework_len);
                    if (moveok) source.moveok = std::string(moveok, moveok_len);
                    if (moveko) source.moveko = std::string(moveko, moveko_len);

                    source.open();
                    arki::nag::verbose("Processing %s...", source.reader->name().c_str());
                    bool success = true;
                    try {
                        auto stats = self->dispatcher->process(*source.reader, source.reader->name());
                        if (status)
                            arki::nag::warning("%s: %s", stats.name.c_str(), stats.summary().c_str());
                        success = stats.success(ignore_duplicates);
                        results.emplace_back(stats);
                    } catch (PythonException&) {
                        throw;
                    } catch (std::exception& e) {
                        arki::nag::warning("%s failed: %s", source.reader->name().c_str(), e.what());
                        success = false;
                        had_exceptions = true;
                    }
                    source.close(success);
                    return true;
                });

                self->processor->end();
            }

            if (had_exceptions)
                return throw_ifnull(PyLong_FromLong(EX_CANTCREAT));

            return throw_ifnull(PyLong_FromLong(compute_result(all_processed, ignore_duplicates, results)));
        } ARKI_CATCH_RETURN_PYO
    }
};


struct ArkiScanDef : public Type<ArkiScanDef, arkipy_ArkiScan>
{
    constexpr static const char* name = "ArkiScan";
    constexpr static const char* qual_name = "arkimet.ArkiScan";
    constexpr static const char* doc = R"(
arki-scan implementation
)";
    GetSetters<> getsetters;
    Methods<set_processor, set_dispatcher, scan_file, scan_sections, dispatch_file, dispatch_sections> methods;

    static void _dealloc(Impl* self)
    {
        self->pool.~shared_ptr<arki::dataset::Pool>();
        delete self->processor;
        delete self->dispatcher;
        Py_TYPE(self)->tp_free(self);
    }

    static PyObject* _str(Impl* self)
    {
        return PyUnicode_FromString(name);
    }

    static PyObject* _repr(Impl* self)
    {
        std::string res = qual_name;
        res += " object";
        return PyUnicode_FromString(res.c_str());
    }

    static int _init(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "session", nullptr };
        arkipy_DatasetSession* session = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O!", const_cast<char**>(kwlist), arkipy_DatasetSession_Type, &session))
            return -1;

        try {
            new (&(self->pool)) std::shared_ptr<arki::dataset::Pool>(session->pool);
            self->processor = nullptr;
            self->dispatcher = nullptr;
        } ARKI_CATCH_RETURN_INT

        return 0;
    }
};

ArkiScanDef* arki_scan_def = nullptr;


}

namespace arki {
namespace python {

void register_arki_scan(PyObject* m)
{
    arki_scan_def = new ArkiScanDef;
    arki_scan_def->define(arkipy_ArkiScan_Type, m);

}

}
}
