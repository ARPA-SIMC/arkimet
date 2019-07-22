#include "config.h"
#include "arki/runtime/processor.h"
#include "arki/runtime.h"
#include "arki/metadata/libarchive.h"
#include "arki/types/source.h"
#include "arki/formatter.h"
#include "arki/dataset.h"
#include "arki/dataset/index/base.h"
#include "arki/emitter/json.h"
#include "arki/targetfile.h"
#include "arki/summary.h"
#include "arki/summary/short.h"
#include "arki/sort.h"
#include "arki/utils/string.h"
#include "arki/summary/stats.h"
#include <cassert>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace {

struct Printer
{
    sys::NamedFileDescriptor& out;
    Printer(sys::NamedFileDescriptor& out) : out(out) {}
    virtual ~Printer() {}
    virtual void operator()(const arki::Metadata& md) = 0;
    virtual void flush() {}
};

struct BinaryPrinter : public Printer
{
    using Printer::Printer;
    void operator()(const arki::Metadata& md) override
    {
        md.write(out);
    }
};

struct LibarchivePrinter : public Printer
{
    arki::metadata::LibarchiveOutput arc_out;

    LibarchivePrinter(sys::NamedFileDescriptor& out, const std::string& format)
        : Printer(out), arc_out(format, out)
    {
    }
    void operator()(const arki::Metadata& md) override
    {
        arc_out.append(md);
    }
    void flush() override
    {
        arc_out.flush();
    }
};

struct FormattedPrinter : public Printer
{
    arki::Formatter* formatter = nullptr;
    FormattedPrinter(sys::NamedFileDescriptor& out, bool annotate)
        : Printer(out)
    {
        if (annotate)
            formatter = arki::Formatter::create().release();
    }
    ~FormattedPrinter() { delete formatter; }
};

struct JSONPrinter : public FormattedPrinter
{
    using FormattedPrinter::FormattedPrinter;
    void operator()(const arki::Metadata& md) override
    {
        stringstream ss;
        arki::emitter::JSON json(ss);
        md.serialise(json, formatter);
        out.write_all_or_throw(ss.str());
    }
};

struct YamlPrinter : public FormattedPrinter
{
    using FormattedPrinter::FormattedPrinter;
    void operator()(const arki::Metadata& md) override
    {
        stringstream ss;
        md.write_yaml(ss, formatter);
        ss << endl;
        out.write_all_or_throw(ss.str());
    }
};

}

namespace arki {
namespace runtime {

typedef std::function<void(const Metadata&)> metadata_print_func;
typedef std::function<void(const Summary&)> summary_print_func;

SingleOutputProcessor::SingleOutputProcessor()
    : output(-1, "(output not initialized yet)")
{
}

SingleOutputProcessor::SingleOutputProcessor(const utils::sys::NamedFileDescriptor& out)
    : output(out, out.name())
{
}


std::unique_ptr<Printer> create_metadata_printer(ProcessorMaker& maker, sys::NamedFileDescriptor& out)
{
    if (!maker.archive.empty())
        return std::unique_ptr<Printer>(new LibarchivePrinter(out, maker.archive));

    if (!maker.json && !maker.yaml && !maker.annotate)
        return std::unique_ptr<Printer>(new BinaryPrinter(out));

    if (maker.json)
        return std::unique_ptr<Printer>(new JSONPrinter(out, maker.annotate));

    return std::unique_ptr<Printer>(new YamlPrinter(out, maker.annotate));
}

summary_print_func create_summary_printer(ProcessorMaker& maker, sys::NamedFileDescriptor& out)
{
    if (!maker.json && !maker.yaml && !maker.annotate)
        return [&](const Summary& s) { s.write(out, out.name()); };

    shared_ptr<Formatter> formatter;
    if (maker.annotate)
        formatter = Formatter::create();

    if (maker.json)
        return [&out, formatter](const Summary& s) {
            stringstream ss;
            emitter::JSON json(ss);
            s.serialise(json, formatter.get());
            string res = ss.str();
            out.write_all_or_throw(ss.str());
        };

    return [&out, formatter](const Summary& s) {
        stringstream ss;
        s.write_yaml(ss, formatter.get());
        ss << endl;
        string res = ss.str();
        out.write_all_or_throw(ss.str());
    };
}

std::string describe_printer(ProcessorMaker& maker)
{
    if (!maker.json && !maker.yaml && !maker.annotate)
        return "binary";

    if (maker.json)
        return "json";

    return "yaml";
}

struct DataProcessor : public SingleOutputProcessor
{
    std::unique_ptr<Printer> printer;
    dataset::DataQuery query;
    vector<string> description_attrs;
    bool data_inline;
    bool server_side;
    bool any_output_generated = false;

    DataProcessor(ProcessorMaker& maker, Matcher& q, const sys::NamedFileDescriptor& out, bool data_inline=false)
        : SingleOutputProcessor(out), printer(create_metadata_printer(maker, output)), query(q, data_inline),
          data_inline(data_inline), server_side(maker.server_side)
    {
        description_attrs.push_back("query=" + q.toString());
        description_attrs.push_back("printer=" + describe_printer(maker));
        if (data_inline)
            description_attrs.push_back("data_inline=true");
        if (maker.server_side)
            description_attrs.push_back("server_side=true");
        if (!maker.sort.empty())
        {
            description_attrs.push_back("sort=" + maker.sort);
            query.sorter = sort::Compare::parse(maker.sort);
        }
    }

    virtual ~DataProcessor() {}

    std::string describe() const override
    {
        string res = "data(";
        res += str::join(", ", description_attrs.begin(), description_attrs.end());
        res += ")";
        return res;
    }

    void check_hooks()
    {
        if (!any_output_generated)
        {
            any_output_generated = true;
        }
    }

    void process(dataset::Reader& ds, const std::string& name) override
    {
        if (data_inline)
        {
            ds.query_data(query, [&](unique_ptr<Metadata> md) { check_hooks(); md->makeInline(); (*printer)(*md); return true; });
        } else if (server_side) {
            if (ds.cfg().has("url"))
            {
                ds.query_data(query, [&](unique_ptr<Metadata> md) {
                    check_hooks();
                    md->set_source(types::Source::createURL(md->source().format, ds.cfg().value("url")));
                    (*printer)(*md);
                    return true;
                });
            } else {
                ds.query_data(query, [&](unique_ptr<Metadata> md) {
                    check_hooks();
                    md->make_absolute();
                    (*printer)(*md);
                    return true;
                });
            }
        } else {
            ds.query_data(query, [&](unique_ptr<Metadata> md) {
                check_hooks();
                md->make_absolute();
                (*printer)(*md);
                return true;
            });
        }
    }

    void end() override
    {
        printer->flush();
    }
};

struct SummaryProcessor : public SingleOutputProcessor
{
    Matcher matcher;
    summary_print_func printer;
    string summary_restrict;
    Summary summary;
    vector<string> description_attrs;

    SummaryProcessor(ProcessorMaker& maker, Matcher& q, const sys::NamedFileDescriptor& out)
        : SingleOutputProcessor(out), matcher(q), printer(create_summary_printer(maker, output))
    {
        description_attrs.push_back("query=" + q.toString());
        if (!maker.summary_restrict.empty())
            description_attrs.push_back("restrict=" + maker.summary_restrict);
        description_attrs.push_back("printer=" + describe_printer(maker));
        summary_restrict = maker.summary_restrict;
    }

    virtual ~SummaryProcessor() {}

    std::string describe() const override
    {
        string res = "summary(";
        res += str::join(", ", description_attrs.begin(), description_attrs.end());
        res += ")";
        return res;
    }

    void process(dataset::Reader& ds, const std::string& name) override
    {
        ds.query_summary(matcher, summary);
    }

    void end() override
    {
        if (!summary_restrict.empty())
        {
            Summary s;
            s.add(summary, dataset::index::parseMetadataBitmask(summary_restrict));
            do_output(s);
        } else
            do_output(summary);
    }

    void do_output(const Summary& s)
    {
        printer(s);
    }
};

struct SummaryShortProcessor : public SingleOutputProcessor
{
    Matcher matcher;
    summary_print_func printer;
    Summary summary;
    bool annotate;
    bool json;

    SummaryShortProcessor(ProcessorMaker& maker, Matcher& q, const sys::NamedFileDescriptor& out)
        : SingleOutputProcessor(out), matcher(q),
          printer(create_summary_printer(maker, output)),
          annotate(maker.annotate),
          json(maker.json)
    {
    }

    virtual ~SummaryShortProcessor() {}

    std::string describe() const override
    {
        return "summary_short";
    }

    void process(dataset::Reader& ds, const std::string& name) override
    {
        ds.query_summary(matcher, summary);
    }

    void end() override
    {
        summary::Short c;
        summary.visit(c);

        shared_ptr<Formatter> formatter;
        if (annotate)
            formatter = Formatter::create();

        stringstream ss;
        if (json)
        {
            emitter::JSON json(ss);
            c.serialise(json, formatter.get());
        }
        else
            c.write_yaml(ss, formatter.get());

        output.write_all_or_retry(ss.str());
    }
};


struct BinaryProcessor : public SingleOutputProcessor
{
    dataset::ByteQuery query;
    vector<string> description_attrs;

    BinaryProcessor(ProcessorMaker& maker, Matcher& q, const sys::NamedFileDescriptor& out)
        : SingleOutputProcessor(out)
    {
        description_attrs.push_back("query=" + q.toString());
        if (!maker.postprocess.empty())
        {
            description_attrs.push_back("postproc=" + maker.postprocess);
            query.setPostprocess(q, maker.postprocess);
#ifdef HAVE_LUA
        } else if (!maker.report.empty()) {
            if (maker.summary)
            {
                description_attrs.push_back("summary_report=" + maker.report);
                query.setRepSummary(q, maker.report);
            } else {
                description_attrs.push_back("data_report=" + maker.report);
                query.setRepMetadata(q, maker.report);
            }
#endif
        } else {
            query.setData(q);
        }

        if (!maker.sort.empty())
        {
            description_attrs.push_back("sort=" + maker.sort);
            query.sorter = sort::Compare::parse(maker.sort);
        }
    }

    std::string describe() const override
    {
        string res = "binary(";
        res += str::join(", ", description_attrs.begin(), description_attrs.end());
        res += ")";
        return res;
    }

    void process(dataset::Reader& ds, const std::string& name) override
    {
        // TODO: validate query's postprocessor with ds' config
        ds.query_bytes(query, output);
    }

    void end() override
    {
    }
};


TargetFileProcessor::TargetFileProcessor(SingleOutputProcessor* next, const std::string& pattern)
    : next(next), pattern(pattern)
{
    description_attrs.push_back("pattern=" + pattern);
    description_attrs.push_back("next=" + next->describe());
}

TargetFileProcessor::~TargetFileProcessor()
{
    if (next) delete next;
}

std::string TargetFileProcessor::describe() const
{
    string res = "targetfile(";
    res += str::join(", ", description_attrs.begin(), description_attrs.end());
    res += ")";
    return res;
}

void TargetFileProcessor::process(dataset::Reader& ds, const std::string& name)
{
    TargetfileSpy spy(ds, next->output, pattern);
    next->process(spy, name);
}

void TargetFileProcessor::end() { next->end(); }


std::unique_ptr<DatasetProcessor> ProcessorMaker::make(Matcher query, sys::NamedFileDescriptor& out)
{
    if (data_only || !postprocess.empty()
#ifdef HAVE_LUA
        || !report.empty()
#endif
        )
        return unique_ptr<DatasetProcessor>(new BinaryProcessor(*this, query, out));
    else if (summary)
        return unique_ptr<DatasetProcessor>(new SummaryProcessor(*this, query, out));
    else if (summary_short)
        return unique_ptr<DatasetProcessor>(new SummaryShortProcessor(*this, query, out));
    else
        return unique_ptr<DatasetProcessor>(new DataProcessor(*this, query, out, data_inline));
}

}
}
