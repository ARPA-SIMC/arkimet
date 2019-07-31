#include "processor.h"
#include "arki/metadata/libarchive.h"
#include "arki/libconfig.h"
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
#include "arki/nag.h"
#include <cassert>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace {

void do_output(sys::NamedFileDescriptor& out, const std::string& str)
{
    out.write_all_or_throw(str);
}

template<typename Output>
struct Printer
{
    std::shared_ptr<Output> out;

    Printer(std::shared_ptr<Output> out) : out(out) {}
    virtual ~Printer() {}
    virtual void operator()(const arki::Metadata& md) = 0;
    virtual void flush() {}
    void output(const std::string& str)
    {
        do_output(*out, str);
    }
};

template<typename Output>
struct BinaryPrinter : public Printer<Output>
{
    using Printer<Output>::Printer;

    void operator()(const arki::Metadata& md) override
    {
        md.write(*(this->out));
    }
};

template<typename Output>
struct LibarchivePrinter : public Printer<Output>
{
    arki::metadata::LibarchiveOutput arc_out;

    LibarchivePrinter(std::shared_ptr<Output> out, const std::string& format)
        : Printer<Output>(out), arc_out(format, *out)
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

template<typename Output>
struct FormattedPrinter : public Printer<Output>
{
    arki::Formatter* formatter = nullptr;
    FormattedPrinter(std::shared_ptr<Output> out, bool annotate)
        : Printer<Output>(out)
    {
        if (annotate)
            formatter = arki::Formatter::create().release();
    }
    ~FormattedPrinter() { delete formatter; }
};

template<typename Output>
struct JSONPrinter : public FormattedPrinter<Output>
{
    using FormattedPrinter<Output>::FormattedPrinter;

    void operator()(const arki::Metadata& md) override
    {
        stringstream ss;
        arki::emitter::JSON json(ss);
        md.serialise(json, this->formatter);
        this->output(ss.str());
    }
};

template<typename Output>
struct YamlPrinter : public FormattedPrinter<Output>
{
    using FormattedPrinter<Output>::FormattedPrinter;

    void operator()(const arki::Metadata& md) override
    {
        this->output(md.to_yaml(this->formatter));
    }
};

}

namespace arki {
namespace python {
namespace cmdline {

typedef std::function<void(const Metadata&)> metadata_print_func;
typedef std::function<void(const Summary&)> summary_print_func;

template<typename Output>
struct SingleOutputProcessor : public DatasetProcessor
{
    std::shared_ptr<Output> output;

    SingleOutputProcessor(std::shared_ptr<Output> out);
};


template<typename Output>
SingleOutputProcessor<Output>::SingleOutputProcessor(std::shared_ptr<Output> out)
    : output(out)
{
}


template<typename Output>
std::unique_ptr<Printer<Output>> create_metadata_printer(ProcessorMaker& maker, std::shared_ptr<Output> out)
{
    if (!maker.archive.empty())
        return std::unique_ptr<Printer<Output>>(new LibarchivePrinter<Output>(out, maker.archive));

    if (!maker.json && !maker.yaml && !maker.annotate)
        return std::unique_ptr<Printer<Output>>(new BinaryPrinter<Output>(out));

    if (maker.json)
        return std::unique_ptr<Printer<Output>>(new JSONPrinter<Output>(out, maker.annotate));

    return std::unique_ptr<Printer<Output>>(new YamlPrinter<Output>(out, maker.annotate));
}

template<typename Output>
summary_print_func create_summary_printer(ProcessorMaker& maker, std::shared_ptr<Output> out)
{
    if (!maker.json && !maker.yaml && !maker.annotate)
        return [&](const Summary& s) { s.write(*out); };

    shared_ptr<Formatter> formatter;
    if (maker.annotate)
        formatter = Formatter::create();

    if (maker.json)
        return [out, formatter](const Summary& s) {
            stringstream ss;
            emitter::JSON json(ss);
            s.serialise(json, formatter.get());
            string res = ss.str();
            do_output(*out, ss.str());
        };

    return [out, formatter](const Summary& s) {
        std::string buf = s.to_yaml(formatter.get());
        buf += "\n";
        do_output(*out, buf);
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

template<typename Output>
struct DataProcessor : public SingleOutputProcessor<Output>
{
    std::unique_ptr<Printer<Output>> printer;
    dataset::DataQuery query;
    vector<string> description_attrs;
    bool data_inline;
    bool server_side;
    bool any_output_generated = false;

    DataProcessor(ProcessorMaker& maker, Matcher& q, std::shared_ptr<Output> out, bool data_inline=false)
        : SingleOutputProcessor<Output>(out), printer(create_metadata_printer(maker, out)), query(q, data_inline),
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

template<typename Output>
struct SummaryProcessor : public SingleOutputProcessor<Output>
{
    Matcher matcher;
    summary_print_func printer;
    string summary_restrict;
    Summary summary;
    vector<string> description_attrs;

    SummaryProcessor(ProcessorMaker& maker, Matcher& q, std::shared_ptr<Output> out)
        : SingleOutputProcessor<Output>(out), matcher(q), printer(create_summary_printer(maker, out))
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

template<typename Output>
struct SummaryShortProcessor : public SingleOutputProcessor<Output>
{
    Matcher matcher;
    summary_print_func printer;
    Summary summary;
    bool annotate;
    bool json;

    SummaryShortProcessor(ProcessorMaker& maker, Matcher& q, std::shared_ptr<Output> out)
        : SingleOutputProcessor<Output>(out), matcher(q),
          printer(create_summary_printer(maker, out)),
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

        do_output(*this->output, ss.str());
    }
};


template<typename Output>
struct BinaryProcessor : public SingleOutputProcessor<Output>
{
    dataset::ByteQuery query;
    vector<string> description_attrs;

    BinaryProcessor(ProcessorMaker& maker, Matcher& q, std::shared_ptr<Output> out)
        : SingleOutputProcessor<Output>(out)
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
        ds.query_bytes(query, *this->output);
    }

    void end() override
    {
    }
};

template<typename Output>
struct TargetFileProcessor : public DatasetProcessor
{
    SingleOutputProcessor<Output>* next;
    std::string pattern;
    std::vector<std::string> description_attrs;

    TargetFileProcessor(SingleOutputProcessor<Output>* next, const std::string& pattern)
    {
        description_attrs.push_back("pattern=" + pattern);
        description_attrs.push_back("next=" + next->describe());
    }

    virtual ~TargetFileProcessor()
    {
        if (next) delete next;
    }

    std::string describe() const override
    {
        string res = "targetfile(";
        res += str::join(", ", description_attrs.begin(), description_attrs.end());
        res += ")";
        return res;
    }

    void process(dataset::Reader& ds, const std::string& name) override
    {
        TargetfileSpy spy(ds, *next->output, pattern);
        next->process(spy, name);
    }

    void end() override
    {
        next->end();
    }
};


std::unique_ptr<DatasetProcessor> ProcessorMaker::make(Matcher query, std::shared_ptr<sys::NamedFileDescriptor> out)
{
    unique_ptr<DatasetProcessor> res;

    if (data_only || !postprocess.empty()
#ifdef HAVE_LUA
        || !report.empty()
#endif
        )
        res.reset(new BinaryProcessor<sys::NamedFileDescriptor>(*this, query, out));
    else if (summary)
        res.reset(new SummaryProcessor<sys::NamedFileDescriptor>(*this, query, out));
    else if (summary_short)
        res.reset(new SummaryShortProcessor<sys::NamedFileDescriptor>(*this, query, out));
    else
        res.reset(new DataProcessor<sys::NamedFileDescriptor>(*this, query, out, data_inline));

    // If targetfile is requested, wrap with the targetfile processor
    if (!targetfile.empty())
    {
        cmdline::SingleOutputProcessor<sys::NamedFileDescriptor>* sop = dynamic_cast<cmdline::SingleOutputProcessor<sys::NamedFileDescriptor>*>(res.release());
        assert(sop != nullptr);
        res.reset(new cmdline::TargetFileProcessor<sys::NamedFileDescriptor>(sop, targetfile));
    }

    return res;
}

template class SingleOutputProcessor<sys::NamedFileDescriptor>;
template class TargetFileProcessor<sys::NamedFileDescriptor>;

}
}
}
