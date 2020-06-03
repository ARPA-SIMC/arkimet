#include "cmdline.h"
#include "cmdline/processor.h"
#include "dataset/progress.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/scan.h"
#include "arki/dataset/session.h"
#include "arki/dataset/fromfunction.h"
#include "arki/nag.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include "matcher.h"
#include "files.h"
#include "dataset.h"

using namespace arki::python;
using namespace arki::utils;

namespace arki {
namespace python {

std::unique_ptr<cmdline::DatasetProcessor> build_processor(std::shared_ptr<arki::dataset::Session> session, PyObject* args, PyObject* kw)
{
    static const char* kwlist[] = {
        "query", "outfile",
        "yaml", "json", "annotate", "inline", "data",
        "summary", "summary_short", "summary_restrict",
        "archive", "postproc", "postproc_data",
        "sort", "progress", nullptr };

    PyObject* py_query = nullptr;
    PyObject* py_outfile = nullptr;
    int yaml = 0, json = 0, annotate = 0, out_inline = 0, data = 0, summary = 0, summary_short = 0;
    const char* summary_restrict = nullptr;
    Py_ssize_t summary_restrict_len;
    PyObject* archive = nullptr;
    const char* postproc = nullptr;
    Py_ssize_t postproc_len;
    PyObject* postproc_data = nullptr;
    const char* sort = nullptr;
    Py_ssize_t sort_len;
    PyObject* progress = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kw, "OO|ppppp" "ppz#" "Oz#O" "z#O", const_cast<char**>(kwlist),
                &py_query, &py_outfile,
                &yaml, &json, &annotate, &out_inline, &data,
                &summary, &summary_short, &summary_restrict, &summary_restrict_len,
                &archive, &postproc, &postproc_len, &postproc_data,
                &sort, &sort_len, &progress))
        throw PythonException();

    arki::Matcher query = matcher_from_python(session, py_query);

    cmdline::ProcessorMaker pmaker;
    // Initialize the processor maker
    pmaker.summary = summary;
    pmaker.summary_short = summary_short;
    pmaker.yaml = yaml;
    pmaker.json = json;
    pmaker.annotate = annotate;
    pmaker.data_only = data;
    pmaker.data_inline = out_inline;
    if (postproc) pmaker.postprocess = std::string(postproc, postproc_len);
    if (summary_restrict) pmaker.summary_restrict = std::string(summary_restrict, summary_restrict_len);
    if (sort) pmaker.sort = std::string(sort, sort_len);
    if (archive && archive != Py_None)
        pmaker.archive = string_from_python(archive);
    pmaker.progress = std::make_shared<dataset::PythonProgress>(progress);

    BinaryOutputFile outfile(py_outfile);

    if (!outfile.fd)
    {
        std::shared_ptr<core::AbstractOutputFile> out(outfile.abstract);
        outfile.abstract = nullptr;
        auto processor = pmaker.make(query, std::move(out));
        return processor;
    } else {
        std::shared_ptr<sys::NamedFileDescriptor> fd(outfile.fd);
        outfile.fd = nullptr;
        auto processor = pmaker.make(query, std::move(fd));
        return processor;
    }
}

bool foreach_file(std::shared_ptr<arki::dataset::Session> session, BinaryInputFile& file, const std::string& format, std::function<void(arki::dataset::Reader&)> dest)
{
    auto scanner = scan::Scanner::get_scanner(format);

    core::cfg::Section cfg;
    cfg.set("format", format);
    cfg.set("name", "stdin:" + scanner->name());
    auto dataset = std::make_shared<arki::dataset::fromfunction::Dataset>(session, cfg);

    auto reader = std::make_shared<arki::dataset::fromfunction::Reader>(dataset);

    if (file.fd)
    {
        reader->generator = [&](metadata_dest_func dest){
            return scanner->scan_pipe(*file.fd, dest);
        };
    } else {
        throw std::runtime_error("scanning abstract input files is not yet supported");
        // reader->generator = [&](metadata_dest_func dest){
        //     return scanner->scan_pipe(*file.abstract, dest);
        // };
    }

    bool success = true;
    try {
        dest(*reader);
    } catch (PythonException& e) {
        throw;
    } catch (std::exception& e) {
        arki::nag::warning("%s failed: %s", reader->name().c_str(), e.what());
        success = false;
    }
    return success;
}

bool foreach_sections(std::shared_ptr<arki::dataset::Session> session, const core::cfg::Sections& inputs, std::function<void(arki::dataset::Reader&)> dest)
{
    bool all_successful = true;
    // Query all the datasets in sequence
    for (auto si: inputs)
    {
        auto reader = session->dataset(si.second)->create_reader();
        bool success = true;
        try {
            dest(*reader);
        } catch (PythonException& e) {
            throw;
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
