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
struct DataProcessor : public DatasetProcessor
{
    std::unique_ptr<Printer<Output>> printer;
    dataset::DataQuery query;
    bool data_inline;
    bool server_side;
    bool any_output_generated = false;

    DataProcessor(std::unique_ptr<Printer<Output>> printer, Matcher& q, std::shared_ptr<Output> out, bool server_side, const std::string& sort, bool data_inline=false)
        : printer(std::move(printer)), query(q, data_inline),
          data_inline(data_inline), server_side(server_side)
    {
        if (!sort.empty())
            query.sorter = sort::Compare::parse(sort);
    }

    virtual ~DataProcessor() {}

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

    SummaryProcessor(Matcher& q, summary_print_func printer, const std::string& summary_restrict, std::shared_ptr<Output> out)
        : SingleOutputProcessor<Output>(out), matcher(q), printer(printer), summary_restrict(summary_restrict)
    {
    }

    virtual ~SummaryProcessor() {}

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

    SummaryShortProcessor(Matcher& q, bool annotate, bool json, summary_print_func printer, std::shared_ptr<Output> out)
        : SingleOutputProcessor<Output>(out), matcher(q),
          printer(printer),
          annotate(annotate),
          json(json)
    {
    }

    virtual ~SummaryShortProcessor() {}

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

    BinaryProcessor(const dataset::ByteQuery& query, std::shared_ptr<Output> out)
        : SingleOutputProcessor<Output>(out), query(query)
    {
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

    TargetFileProcessor(SingleOutputProcessor<Output>* next, const std::string& pattern)
    {
    }

    virtual ~TargetFileProcessor()
    {
        if (next) delete next;
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


std::unique_ptr<DatasetProcessor> ProcessorMaker::make(Matcher matcher, std::shared_ptr<sys::NamedFileDescriptor> out)
{
    unique_ptr<DatasetProcessor> res;

    if (data_only || !postprocess.empty()
#ifdef HAVE_LUA
        || !report.empty()
#endif
        )
    {
        arki::dataset::ByteQuery query;
        if (!postprocess.empty())
        {
            query.setPostprocess(matcher, postprocess);
#ifdef HAVE_LUA
        } else if (!report.empty()) {
            if (summary)
                query.setRepSummary(matcher, report);
            else
                query.setRepMetadata(matcher, report);
#endif
        } else
            query.setData(matcher);

        if (!sort.empty())
            query.sorter = sort::Compare::parse(sort);

        res.reset(new BinaryProcessor<sys::NamedFileDescriptor>(query, out));
    }
    else if (summary || summary_short)
    {
        summary_print_func printer;
        if (!json && !yaml && !annotate)
            printer = [out](const Summary& s) { s.write(*out); };
        else {
            shared_ptr<Formatter> formatter;
            if (annotate)
                formatter = Formatter::create();

            if (json)
                printer = [out, formatter](const Summary& s) {
                    stringstream ss;
                    emitter::JSON json(ss);
                    s.serialise(json, formatter.get());
                    string res = ss.str();
                    do_output(*out, ss.str());
                };
            else
                printer = [out, formatter](const Summary& s) {
                    std::string buf = s.to_yaml(formatter.get());
                    buf += "\n";
                    do_output(*out, buf);
                };
        }

        if (summary)
            res.reset(new SummaryProcessor<sys::NamedFileDescriptor>(matcher, printer, summary_restrict, out));
        else
            res.reset(new SummaryShortProcessor<sys::NamedFileDescriptor>(matcher, annotate, json, printer, out));
    }
    else
    {
        std::unique_ptr<Printer<sys::NamedFileDescriptor>> printer;

        if (!archive.empty())
            printer = std::unique_ptr<Printer<sys::NamedFileDescriptor>>(new LibarchivePrinter<sys::NamedFileDescriptor>(out, archive));
        else if (!json && !yaml && !annotate)
            printer = std::unique_ptr<Printer<sys::NamedFileDescriptor>>(new BinaryPrinter<sys::NamedFileDescriptor>(out));
        else if (json)
            printer = std::unique_ptr<Printer<sys::NamedFileDescriptor>>(new JSONPrinter<sys::NamedFileDescriptor>(out, annotate));
        else
            printer = std::unique_ptr<Printer<sys::NamedFileDescriptor>>(new YamlPrinter<sys::NamedFileDescriptor>(out, annotate));

        res.reset(new DataProcessor<sys::NamedFileDescriptor>(std::move(printer), matcher, out, server_side, sort, data_inline));
    }

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
