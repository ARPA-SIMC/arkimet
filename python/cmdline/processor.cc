#include "processor.h"
#include "arki/metadata.h"
#include "arki/metadata/archive.h"
#include "arki/libconfig.h"
#include "arki/types/source.h"
#include "arki/formatter.h"
#include "arki/stream.h"
#include "arki/dataset.h"
#include "arki/dataset/query.h"
#include "arki/dataset/index/base.h"
#include "arki/structured/json.h"
#include "arki/structured/keys.h"
#include "arki/summary.h"
#include "arki/summary/short.h"
#include "arki/metadata/sort.h"
#include "arki/utils/string.h"
#include "arki/core/file.h"
#include "arki/summary/stats.h"
#include "arki/nag.h"
#include "python/dataset/progress.h"
#include <cassert>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace python {
namespace cmdline {

typedef std::function<void(const Metadata&)> metadata_print_func;
typedef std::function<void(const Summary&)> summary_print_func;


struct DataProcessor : public DatasetProcessor
{
    arki::dataset::DataQuery query;
    metadata_print_func printer;
    bool data_inline;
    bool server_side;

    DataProcessor(const arki::dataset::DataQuery& query, metadata_print_func printer, bool server_side, bool data_inline=false)
        : query(query), printer(std::move(printer)),
          data_inline(data_inline), server_side(server_side)
    {
    }

    virtual ~DataProcessor() {}

    void process(arki::dataset::Reader& reader, const std::string& name) override
    {
        arki::nag::verbose("Processing %s...", reader.name().c_str());
        if (data_inline)
        {
            reader.query_data(query, [&](std::shared_ptr<Metadata> md) { md->makeInline(); printer(*md); return true; });
        } else if (server_side) {
            if (reader.config()->has("url"))
            {
                reader.query_data(query, [&](std::shared_ptr<Metadata> md) {
                    md->set_source(types::Source::createURL(md->source().format, reader.config()->value("url")));
                    printer(*md);
                    return true;
                });
            } else {
                reader.query_data(query, [&](std::shared_ptr<Metadata> md) {
                    md->make_absolute();
                    printer(*md);
                    return true;
                });
            }
        } else {
            reader.query_data(query, [&](std::shared_ptr<Metadata> md) {
                md->make_absolute();
                printer(*md);
                return true;
            });
        }
    }
};


struct LibarchiveProcessor : public DatasetProcessor
{
    arki::dataset::DataQuery query;
    std::shared_ptr<arki::metadata::ArchiveOutput> arc_out;

    LibarchiveProcessor(
            Matcher matcher, std::shared_ptr<sys::NamedFileDescriptor> out,
            const std::string& format, std::shared_ptr<arki::dataset::QueryProgress> progress)
        : query(matcher, true), arc_out(arki::metadata::ArchiveOutput::create(format, out))
    {
        query.progress = progress;
    }

    virtual ~LibarchiveProcessor() {}

    void process(arki::dataset::Reader& reader, const std::string& name) override
    {
        arki::nag::verbose("Processing %s...", reader.name().c_str());
        reader.query_data(query, [&](std::shared_ptr<Metadata> md) { arc_out->append(*md); return true; });
    }

    void end() override
    {
        arc_out->flush(true);
    }
};


struct SummaryProcessor : public DatasetProcessor
{
    std::shared_ptr<StreamOutput> output;
    Matcher matcher;
    summary_print_func printer;
    string summary_restrict;
    Summary summary;

    SummaryProcessor(Matcher& q, summary_print_func printer, const std::string& summary_restrict, std::shared_ptr<StreamOutput> out)
        : output(out), matcher(q), printer(printer), summary_restrict(summary_restrict)
    {
    }

    virtual ~SummaryProcessor() {}

    void process(arki::dataset::Reader& reader, const std::string& name) override
    {
        arki::nag::verbose("Processing %s...", reader.name().c_str());
        reader.query_summary(matcher, summary);
    }

    void end() override
    {
        if (!summary_restrict.empty())
        {
            Summary s;
            s.add(summary, arki::dataset::index::parseMetadataBitmask(summary_restrict));
            do_output(s);
        } else
            do_output(summary);
    }

    void do_output(const Summary& s)
    {
        printer(s);
    }
};

struct SummaryShortProcessor : public DatasetProcessor
{
    std::shared_ptr<StreamOutput> output;
    Matcher matcher;
    summary_print_func printer;
    Summary summary;
    bool annotate;
    bool json;

    SummaryShortProcessor(Matcher& q, bool annotate, bool json, summary_print_func printer, std::shared_ptr<StreamOutput> out)
        : output(out), matcher(q),
          printer(printer),
          annotate(annotate),
          json(json)
    {
    }

    virtual ~SummaryShortProcessor() {}

    void process(arki::dataset::Reader& reader, const std::string& name) override
    {
        arki::nag::verbose("Processing %s...", reader.name().c_str());
        reader.query_summary(matcher, summary);
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
            structured::JSON json(ss);
            c.serialise(json, structured::keys_json, formatter.get());
        }
        else
            c.write_yaml(ss, formatter.get());

        this->output->send_buffer(ss.str().data(), ss.str().size());
    }
};


struct BinaryProcessor : public DatasetProcessor
{
    std::shared_ptr<StreamOutput> output;
    arki::dataset::ByteQuery query;

    BinaryProcessor(const arki::dataset::ByteQuery& query, std::shared_ptr<StreamOutput> out)
        : output(out), query(query)
    {
    }

    void process(arki::dataset::Reader& reader, const std::string& name) override
    {
        arki::nag::verbose("Processing %s...", reader.name().c_str());
        // TODO: validate query's postprocessor with reader' config
        reader.query_bytes(query, *this->output);
    }
};


std::unique_ptr<DatasetProcessor> ProcessorMaker::make_binary(Matcher matcher, std::shared_ptr<StreamOutput> out)
{
    // Binary output from the dataset
    arki::dataset::ByteQuery query;
    if (!postprocess.empty())
    {
        query.setPostprocess(matcher, postprocess);
    } else {
        query.setData(matcher);
    }

    if (!sort.empty())
        query.sorter = metadata::sort::Compare::parse(sort);
    query.progress = progress;

    return unique_ptr<DatasetProcessor>(new BinaryProcessor(query, out));
}

std::unique_ptr<DatasetProcessor> ProcessorMaker::make_summary(Matcher matcher, std::shared_ptr<StreamOutput> out)
{
    // Summary output from the dataset
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
                structured::JSON json(ss);
                s.serialise(json, structured::keys_json, formatter.get());
                string res = ss.str();
                out->send_buffer(ss.str().data(), ss.str().size());
            };
        else
            printer = [out, formatter](const Summary& s) {
                std::string buf = s.to_yaml(formatter.get());
                buf += "\n";
                out->send_buffer(buf.data(), buf.size());
            };
    }

    if (summary)
        return std::unique_ptr<DatasetProcessor>(new SummaryProcessor(matcher, printer, summary_restrict, out));
    else
        return std::unique_ptr<DatasetProcessor>(new SummaryShortProcessor(matcher, annotate, json, printer, out));
}

std::unique_ptr<DatasetProcessor> ProcessorMaker::make_metadata(Matcher matcher, std::shared_ptr<StreamOutput> out)
{
    // Metadata output from the dataset
    metadata_print_func printer;

    std::shared_ptr<Formatter> formatter;
    if (annotate)
        formatter = Formatter::create();

    if (!json && !yaml && !annotate)
        printer = [out](const arki::Metadata& md) { md.write(*out); };
    else if (json)
        printer = [out, formatter](const arki::Metadata& md) {
            stringstream ss;
            arki::structured::JSON json(ss);
            md.serialise(json, structured::keys_json, formatter.get());
            out->send_buffer(ss.str().data(), ss.str().size());
        };
    else
        printer = [out, formatter](const arki::Metadata& md) {
            std::string yaml = md.to_yaml(formatter.get());
            yaml.push_back('\n');
            out->send_buffer(yaml.data(), yaml.size());
        };

    arki::dataset::DataQuery query(matcher, data_inline);
    if (!sort.empty())
        query.sorter = metadata::sort::Compare::parse(sort);
    query.progress = progress;

    return std::unique_ptr<DatasetProcessor>(new DataProcessor(query, printer, server_side, data_inline));
}

std::unique_ptr<DatasetProcessor> ProcessorMaker::make(Matcher matcher, std::shared_ptr<StreamOutput> out)
{
    if (!progress)
        progress = std::make_shared<python::dataset::PythonProgress>();

    if (data_only || !postprocess.empty())
        return make_binary(matcher, out);
    else if (summary || summary_short)
        return make_summary(matcher, out);
    else
        return make_metadata(matcher, out);
}

std::unique_ptr<DatasetProcessor> ProcessorMaker::make_libarchive(Matcher matcher, std::shared_ptr<core::NamedFileDescriptor> out, std::string archive, std::shared_ptr<arki::dataset::QueryProgress> progress)
{
    return std::unique_ptr<DatasetProcessor>(new LibarchiveProcessor(matcher, out, archive, progress));
}

}
}
}
