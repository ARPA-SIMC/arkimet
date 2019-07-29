#include "arki-scan.h"
#include "arki/nag.h"
#include "arki-scan/dispatch.h"
#include "arki/dispatcher.h"
#include "arki/utils/string.h"
#include "arki/exceptions.h"
#include "arki/dataset/file.h"
#include "arki/validator.h"
#include "arki/utils.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include "cmdline.h"
#include "cmdline/processor.h"

using namespace arki::python;
using namespace arki::utils;

extern "C" {

PyTypeObject* arkipy_ArkiScan_Type = nullptr;

}


namespace {

static std::string moveFile(const std::string& source, const std::string& targetdir)
{
    std::string targetFile = str::joinpath(targetdir, str::basename(source));
    if (::rename(source.c_str(), targetFile.c_str()) == -1)
        arki::throw_system_error ("cannot move " + source + " to " + targetFile);
    return targetFile;
}

static std::string moveFile(const arki::dataset::Reader& ds, const std::string& targetdir)
{
    if (const arki::dataset::File* d = dynamic_cast<const arki::dataset::File*>(&ds))
        return moveFile(d->config().pathname, targetdir);
    else
        return std::string();
}

/**
 * Data source from the path to a file or dataset
 */
struct FileSource
{
    std::shared_ptr<arki::dataset::Reader> reader;

    arki::core::cfg::Section cfg;
    std::string movework;
    std::string moveok;
    std::string moveko;

    FileSource(const arki::core::cfg::Section& info)
        : cfg(info)
    {
    }

    void open()
    {
        if (!movework.empty() && cfg.value("type") == "file")
            cfg.set("path", moveFile(cfg.value("path"), movework));
        reader = arki::dataset::Reader::create(cfg);
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
std::unique_ptr<arki_scan::MetadataDispatch> build_dispatcher(cmdline::DatasetProcessor& processor, PyObject* args, PyObject* kw)
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
    const char* flush_threshold = nullptr;
    Py_ssize_t flush_threshold_len;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "|z#z#z#OOz#", const_cast<char**>(kwlist),
                &copyok, &copyok_len, &copyko, &copyko_len,
                &validate, &validate_len,
                &dispatch, &testdispatch,
                &flush_threshold, &flush_threshold_len
                ))
        throw PythonException();

    std::unique_ptr<arki_scan::MetadataDispatch> res(new arki_scan::MetadataDispatch(processor));

    if (testdispatch)
    {
        res->dispatcher = new arki::TestDispatcher(sections_from_python(testdispatch));
    }
    else if (dispatch)
    {
        res->dispatcher = new arki::RealDispatcher(sections_from_python(dispatch));
    }
    else
        throw std::runtime_error("cannot create MetadataDispatch with no --dispatch or --testdispatch information");

    if (validate)
    {
        const arki::ValidatorRepository& vals = arki::ValidatorRepository::get();

        // Add validators to dispatcher
        str::Split splitter(std::string(validate, validate_len), ",");
        for (str::Split::const_iterator iname = splitter.begin();
                iname != splitter.end(); ++iname)
        {
            arki::ValidatorRepository::const_iterator i = vals.find(*iname);
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
        res->flush_threshold = parse_size(std::string(flush_threshold, flush_threshold_len));

    return res;
}

struct set_inputs : public MethKwargs<set_inputs, arkipy_ArkiScan>
{
    constexpr static const char* name = "set_inputs";
    constexpr static const char* signature = "config: arkimet.cfg.Sections";
    constexpr static const char* returns = "";
    constexpr static const char* summary = "set input configuration";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "config", nullptr };
        PyObject* py_config = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "O", const_cast<char**>(kwlist), &py_config))
            return nullptr;

        try {
            self->inputs = sections_from_python(py_config);
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

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
            auto processor = build_processor(args, kw);
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
            auto dispatcher = build_dispatcher(*(self->processor), args, kw);
            self->dispatcher = dispatcher.release();
            Py_RETURN_NONE;
        } ARKI_CATCH_RETURN_PYO
    }
};

struct scan_stdin : public MethKwargs<scan_stdin, arkipy_ArkiScan>
{
    constexpr static const char* name = "scan_stdin";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-scan --stdin";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "format", nullptr };

        const char* format = nullptr;
        Py_ssize_t format_len;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "z#", const_cast<char**>(kwlist),
                    &format, &format_len))
            return nullptr;

        try {
            bool all_successful;
            {
                ReleaseGIL rg;
                all_successful = foreach_stdin(std::string(format, format_len), [&](arki::dataset::Reader& reader) {
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
                all_successful = foreach_sections(self->inputs, [&](arki::dataset::Reader& reader) {
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

struct dispatch_stdin : public MethKwargs<dispatch_stdin, arkipy_ArkiScan>
{
    constexpr static const char* name = "dispatch_stdin";
    constexpr static const char* signature = "";
    constexpr static const char* returns = "int";
    constexpr static const char* summary = "run arki-scan --stdin --dispatch";
    constexpr static const char* doc = nullptr;

    static PyObject* run(Impl* self, PyObject* args, PyObject* kw)
    {
        static const char* kwlist[] = { "format", "ignore_duplicates", "status", nullptr };

        const char* format = nullptr;
        Py_ssize_t format_len;
        int ignore_duplicates = 0;
        int status = 0;
        if (!PyArg_ParseTupleAndKeywords(args, kw, "z#|pp", const_cast<char**>(kwlist),
                    &format, &format_len, &ignore_duplicates, &status))
            return nullptr;

        try {
            bool all_successful;
            {
                ReleaseGIL rg;

                bool dispatch_ok = true;
                bool res = foreach_stdin(std::string(format, format_len), [&](arki::dataset::Reader& reader) {
                    auto stats = self->dispatcher->process(reader, reader.name());

                    if (status)
                    {
                        fprintf(stderr, "%s: %s\n", stats.name.c_str(), stats.summary().c_str());
                        fflush(stderr);
                    }

                    dispatch_ok = stats.success(ignore_duplicates) && dispatch_ok;
                });

                all_successful = res && dispatch_ok;

                self->processor->end();
            }
            if (all_successful)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
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
            bool all_successful = true;

            {
                ReleaseGIL rg;

                // Query all the datasets in sequence
                for (auto si: self->inputs)
                {
                    FileSource source(si.second);
                    if (movework) source.movework = std::string(movework, movework_len);
                    if (moveok) source.moveok = std::string(moveok, moveok_len);
                    if (moveko) source.moveko = std::string(moveko, moveko_len);

                    source.open();
                    arki::nag::verbose("Processing %s...", source.reader->name().c_str());
                    bool success = true;
                    try {
                        auto stats = self->dispatcher->process(*source.reader, source.reader->name());
                        if (status)
                        {
                            fprintf(stderr, "%s: %s\n", stats.name.c_str(), stats.summary().c_str());
                            fflush(stderr);
                        }
                        success = stats.success(ignore_duplicates);
                    } catch (std::exception& e) {
                        arki::nag::warning("%s failed: %s", source.reader->name().c_str(), e.what());
                        success = false;
                    }
                    source.close(success);
                    if (!success) all_successful = false;
                }

                self->processor->end();
            }
            if (all_successful)
                Py_RETURN_TRUE;
            else
                Py_RETURN_FALSE;
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
    Methods<set_inputs, set_processor, set_dispatcher, scan_stdin, scan_sections, dispatch_stdin, dispatch_sections> methods;

    static void _dealloc(Impl* self)
    {
        self->inputs.~Sections();
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
        static const char* kwlist[] = { nullptr };
        if (!PyArg_ParseTupleAndKeywords(args, kw, "", const_cast<char**>(kwlist)))
            return -1;

        try {
            new (&(self->inputs)) arki::core::cfg::Sections;
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
