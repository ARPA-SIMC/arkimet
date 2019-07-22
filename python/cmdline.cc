#include "arki/runtime/processor.h"
#include "arki/runtime/source.h"
#include "arki/runtime/dispatch.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/matcher.h"
#include "arki/validator.h"
#include "arki/dispatcher.h"
#include "arki/utils.h"
#include "arki/utils/string.h"
#include "arki-query.h"
#include "utils/core.h"
#include "utils/methods.h"
#include "utils/type.h"
#include "utils/values.h"
#include "common.h"
#include <iostream>

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

std::unique_ptr<runtime::MetadataDispatch> build_dispatcher(runtime::DatasetProcessor& processor, PyObject* args, PyObject* kw)
{
//        self.parser_dis.add_argument("--status", action="store_true",
//                                     help="print to standard error a line per every file with a summary"
//                                          " of how it was handled")
//
//        self.parser_dis.add_argument("--flush-threshold", metavar="size",
//                                     help="import a batch as soon as the data read so far exceeds"
//                                          " this amount of megabytes (default: 128Mi; use 0 to load all"
//                                          " in RAM no matter what)")
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

    std::unique_ptr<runtime::MetadataDispatch> res(new runtime::MetadataDispatch(processor));

    if (testdispatch)
    {
        res->dispatcher = new TestDispatcher(sections_from_python(testdispatch), std::cerr);
    }
    else if (dispatch)
    {
        res->dispatcher = new RealDispatcher(sections_from_python(dispatch));
    }
    else
        throw std::runtime_error("cannot create MetadataDispatch with no --dispatch or --testdispatch information");

    if (validate)
    {
        const ValidatorRepository& vals = ValidatorRepository::get();

        // Add validators to dispatcher
        str::Split splitter(std::string(validate, validate_len), ",");
        for (str::Split::const_iterator iname = splitter.begin();
                iname != splitter.end(); ++iname)
        {
            ValidatorRepository::const_iterator i = vals.find(*iname);
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

}
}
