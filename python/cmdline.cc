#include "arki/runtime/processor.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/scan.h"
#include "arki/dataset/fromfunction.h"
#include "arki/nag.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"

using namespace arki::python;
using namespace arki::utils;

namespace arki {
namespace python {

std::unique_ptr<runtime::DatasetProcessor> build_processor(PyObject* args, PyObject* kw)
{
    static const char* kwlist[] = {
        "query", "outfile",
        "yaml", "json", "annotate", "inline", "data",
        "summary", "summary_short",
        "report", "summary_restrict",
        "archive", "postproc", "postproc_data",
        "sort", "targetfile", nullptr };

    PyObject* py_query = nullptr;
    const char* outfile = nullptr;
    Py_ssize_t outfile_len;
    int yaml = 0, json = 0, annotate = 0, out_inline = 0, data = 0, summary = 0, summary_short = 0;
    const char* report = nullptr;
    Py_ssize_t report_len;
    const char* summary_restrict = nullptr;
    Py_ssize_t summary_restrict_len;
    PyObject* archive = nullptr;
    const char* postproc = nullptr;
    Py_ssize_t postproc_len;
    PyObject* postproc_data = nullptr;
    const char* sort = nullptr;
    Py_ssize_t sort_len;
    const char* targetfile = nullptr;
    Py_ssize_t targetfile_len;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os#|ppppp" "pp" "z#z#" "Oz#O" "z#z#", const_cast<char**>(kwlist),
                &py_query, &outfile, &outfile_len,
                &yaml, &json, &annotate, &out_inline, &data,
                &summary, &summary_short,
                &report, &report_len, &summary_restrict, &summary_restrict_len,
                &archive, &postproc, &postproc_len, &postproc_data,
                &sort, &sort_len, &targetfile, &targetfile_len))
        throw PythonException();

    std::unique_ptr<sys::NamedFileDescriptor> output;
    auto pathname = std::string(outfile, outfile_len);
    if (pathname != "-")
        output.reset(new sys::File(pathname, O_WRONLY | O_CREAT | O_TRUNC));
    else
        output.reset(new arki::core::Stdout);

    arki::Matcher query = matcher_from_python(py_query);

    arki::runtime::ProcessorMaker pmaker;
    // Initialize the processor maker
    pmaker.summary = summary;
    pmaker.summary_short = summary_short;
    pmaker.yaml = yaml;
    pmaker.json = json;
    pmaker.annotate = annotate;
    pmaker.data_only = data;
    pmaker.data_inline = out_inline;
    if (postproc) pmaker.postprocess = std::string(postproc, postproc_len);
    if (report) pmaker.report = std::string(report, report_len);
    if (summary_restrict) pmaker.summary_restrict = std::string(summary_restrict, summary_restrict_len);
    if (sort) pmaker.sort = std::string(sort, sort_len);
    if (archive && archive != Py_None)
        pmaker.archive = string_from_python(archive);

    auto processor = pmaker.make(query, *output);

    // If targetfile is requested, wrap with the targetfile processor
    if (targetfile)
    {
        arki::runtime::SingleOutputProcessor* sop = dynamic_cast<arki::runtime::SingleOutputProcessor*>(processor.release());
        assert(sop != nullptr);
        processor.reset(new arki::runtime::TargetFileProcessor(sop, std::string(targetfile, targetfile_len)));
    }

    return processor;
}

bool foreach_stdin(const std::string& format, std::function<void(dataset::Reader&)> dest)
{
    auto scanner = scan::Scanner::get_scanner(format);

    core::cfg::Section cfg;
    cfg.set("format", format);
    cfg.set("name", "stdin:" + scanner->name());
    auto config = dataset::fromfunction::Config::create(cfg);

    auto reader = std::make_shared<dataset::fromfunction::Reader>(config);
    reader->generator = [&](metadata_dest_func dest){
        sys::NamedFileDescriptor fd_stdin(0, "stdin");
        return scanner->scan_pipe(fd_stdin, dest);
    };

    bool success = true;
    try {
        dest(*reader);
    } catch (std::exception& e) {
        arki::nag::warning("%s failed: %s", reader->name().c_str(), e.what());
        success = false;
    }
    return success;
}

bool foreach_sections(const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest)
{
    bool all_successful = true;
    // Query all the datasets in sequence
    for (auto si: inputs)
    {
        auto reader = dataset::Reader::create(si.second);
        bool success = true;
        try {
            dest(*reader);
        } catch (std::exception& e) {
            arki::nag::warning("%s failed: %s", reader->name().c_str(), e.what());
            success = false;
        }
        if (!success) all_successful = false;
    }
    return all_successful;
}

}
}
