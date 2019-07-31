#include "processor.h"
#include "arki/metadata/libarchive.h"
#include "arki/libconfig.h"
#include "arki/types/source.h"
#include "arki/formatter.h"
#include "arki/dataset.h"
#include "arki/dataset/index/base.h"
#include "arki/emitter/json.h"
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


}

namespace arki {
namespace python {
namespace cmdline {

typedef std::function<void(const Metadata&)> metadata_print_func;
typedef std::function<void(const Summary&)> summary_print_func;


struct DataProcessor : public DatasetProcessor
{
    dataset::DataQuery query;
    metadata_print_func printer;
    bool data_inline;
    bool server_side;

    DataProcessor(const dataset::DataQuery& query, metadata_print_func printer, bool server_side, bool data_inline=false)
        : query(query), printer(std::move(printer)),
          data_inline(data_inline), server_side(server_side)
    {
    }

    virtual ~DataProcessor() {}

    void process(dataset::Reader& ds, const std::string& name) override
    {
        if (data_inline)
        {
            ds.query_data(query, [&](unique_ptr<Metadata> md) { md->makeInline(); printer(*md); return true; });
        } else if (server_side) {
            if (ds.cfg().has("url"))
            {
                ds.query_data(query, [&](unique_ptr<Metadata> md) {
                    md->set_source(types::Source::createURL(md->source().format, ds.cfg().value("url")));
                    printer(*md);
                    return true;
                });
            } else {
                ds.query_data(query, [&](unique_ptr<Metadata> md) {
                    md->make_absolute();
                    printer(*md);
                    return true;
                });
            }
        } else {
            ds.query_data(query, [&](unique_ptr<Metadata> md) {
                md->make_absolute();
                printer(*md);
                return true;
            });
        }
    }
};


template<typename Output>
struct LibarchiveProcessor : public DatasetProcessor
{
    dataset::DataQuery query;
    arki::metadata::LibarchiveOutput arc_out;

    LibarchiveProcessor(Matcher matcher, std::shared_ptr<Output> out, const std::string& format)
        : query(matcher, true), arc_out(format, *out)
    {
    }

    virtual ~LibarchiveProcessor() {}

    void process(dataset::Reader& ds, const std::string& name) override
    {
        ds.query_data(query, [&](unique_ptr<Metadata> md) { arc_out.append(*md); return true; });
    }

    void end() override
    {
        arc_out.flush();
    }
};


template<typename Output>
struct SummaryProcessor : public DatasetProcessor
{
    std::shared_ptr<Output> output;
    Matcher matcher;
    summary_print_func printer;
    string summary_restrict;
    Summary summary;

    SummaryProcessor(Matcher& q, summary_print_func printer, const std::string& summary_restrict, std::shared_ptr<Output> out)
        : output(out), matcher(q), printer(printer), summary_restrict(summary_restrict)
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
struct SummaryShortProcessor : public DatasetProcessor
{
    std::shared_ptr<Output> output;
    Matcher matcher;
    summary_print_func printer;
    Summary summary;
    bool annotate;
    bool json;

    SummaryShortProcessor(Matcher& q, bool annotate, bool json, summary_print_func printer, std::shared_ptr<Output> out)
        : output(out), matcher(q),
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
struct BinaryProcessor : public DatasetProcessor
{
    std::shared_ptr<Output> output;
    dataset::ByteQuery query;

    BinaryProcessor(const dataset::ByteQuery& query, std::shared_ptr<Output> out)
        : output(out), query(query)
    {
    }

    void process(dataset::Reader& ds, const std::string& name) override
    {
        // TODO: validate query's postprocessor with ds' config
        ds.query_bytes(query, *this->output);
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
    else if (!archive.empty())
    {
        res.reset(new LibarchiveProcessor<sys::NamedFileDescriptor>(matcher, out, archive));
    }
    else
    {
        metadata_print_func printer;

        std::shared_ptr<Formatter> formatter;
        if (annotate)
            formatter = Formatter::create();

        if (!json && !yaml && !annotate)
            printer = [out](const arki::Metadata& md) { md.write(*out); };
        else if (json)
            printer = [out, formatter](const arki::Metadata& md) {
                stringstream ss;
                arki::emitter::JSON json(ss);
                md.serialise(json, formatter.get());
                do_output(*out, ss.str());
            };
        else
            printer = [out, formatter](const arki::Metadata& md) {
                do_output(*out, md.to_yaml(formatter.get()));
            };

        dataset::DataQuery query(matcher, data_inline);
        if (!sort.empty())
            query.sorter = sort::Compare::parse(sort);
        res.reset(new DataProcessor(query, printer, server_side, data_inline));
    }

    return res;
}

}
}
}
