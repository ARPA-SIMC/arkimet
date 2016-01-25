#include "config.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/io.h"
#include "arki/formatter.h"
#include "arki/dataset.h"
#include "arki/dataset/index/base.h"
#include "arki/emitter/json.h"
#include "arki/targetfile.h"
#include "arki/summary.h"
#include "arki/sort.h"
#include "arki/utils/string.h"
#include "arki/types/typeset.h"

using namespace std;
using namespace arki::utils;

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


metadata_print_func create_metadata_printer(ProcessorMaker& maker, sys::NamedFileDescriptor& out)
{
    if (!maker.json && !maker.yaml && !maker.annotate)
        return [&](const Metadata& md) { md.write(out); };

    shared_ptr<Formatter> formatter;
    if (maker.annotate)
        formatter = Formatter::create();

    if (maker.json)
        return [&out, formatter](const Metadata& md) {
            stringstream ss;
            emitter::JSON json(ss);
            md.serialise(json, formatter.get());
            out.write_all_or_throw(ss.str());
        };

    return [&out, formatter](const Metadata& md) {
        stringstream ss;
        md.writeYaml(ss, formatter.get());
        ss << endl;
        out.write_all_or_throw(ss.str());
    };
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
        s.writeYaml(ss, formatter.get());
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
    metadata_print_func printer;
    dataset::DataQuery query;
    vector<string> description_attrs;
    bool data_inline;
    bool server_side;
    std::function<void()> data_start_hook;
    bool any_output_generated = false;

    DataProcessor(ProcessorMaker& maker, Matcher& q, const sys::NamedFileDescriptor& out, bool data_inline=false)
        : SingleOutputProcessor(out), printer(create_metadata_printer(maker, output)), query(q, data_inline),
          data_inline(data_inline), server_side(maker.server_side), data_start_hook(maker.data_start_hook)
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
            if (data_start_hook) data_start_hook();
            any_output_generated = true;
        }
    }

    void process(dataset::Reader& ds, const std::string& name) override
    {
        if (data_inline)
        {
            ds.query_data(query, [&](unique_ptr<Metadata> md) { check_hooks(); md->makeInline(); printer(*md); return true; });
        } else if (server_side) {
            map<string, string>::const_iterator iurl = ds.cfg().find("url");
            if (iurl == ds.cfg().end())
            {
                ds.query_data(query, [&](unique_ptr<Metadata> md) {
                    check_hooks();
                    md->make_absolute();
                    printer(*md);
                    return true;
                });
            } else {
                ds.query_data(query, [&](unique_ptr<Metadata> md) {
                    check_hooks();
                    md->set_source(types::Source::createURL(md->source().format, iurl->second));
                    printer(*md);
                    return true;
                });
            }
        } else {
            ds.query_data(query, [&](unique_ptr<Metadata> md) {
                check_hooks();
                md->make_absolute();
                printer(*md);
                return true;
            });
        }
    }

    void end() override
    {
    }
};

struct SummaryProcessor : public SingleOutputProcessor
{
    Matcher matcher;
    summary_print_func printer;
    string summary_restrict;
    Summary summary;
    vector<string> description_attrs;
    std::function<void()> data_start_hook;

    SummaryProcessor(ProcessorMaker& maker, Matcher& q, const sys::NamedFileDescriptor& out)
        : SingleOutputProcessor(out), matcher(q), printer(create_summary_printer(maker, output)), data_start_hook(maker.data_start_hook)
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
        if (data_start_hook) data_start_hook();
        printer(s);
    }
};

namespace {

struct MDCollector : public summary::Visitor
{
    std::map<types::Code, types::TypeSet> items;

    bool operator()(const std::vector<const types::Type*>& md, const summary::Stats& stats) override
    {
        for (size_t i = 0; i < md.size(); ++i)
        {
            if (!md[i]) continue;
            types::Code code = codeForPos(i);
            items[code].insert(*md[i]);
        }
        return true;
    }
};

}

struct SummaryShortProcessor : public SingleOutputProcessor
{
    Matcher matcher;
    summary_print_func printer;
    Summary summary;
    std::function<void()> data_start_hook;

    SummaryShortProcessor(ProcessorMaker& maker, Matcher& q, const sys::NamedFileDescriptor& out)
        : SingleOutputProcessor(out), matcher(q), printer(create_summary_printer(maker, output)), data_start_hook(maker.data_start_hook)
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
        if (data_start_hook) data_start_hook();
        MDCollector c;
        summary.visit(c);
        stringstream ss;
        ss << "Size: " << summary.size() << endl;
        ss << "Count: " << summary.count() << endl;
        ss << "Reftime: " << *summary.getReferenceTime() << endl;
        for (const auto& i: c.items)
        {
            ss << types::formatCode(i.first) << ":" << endl;
            for (const auto& mi: i.second)
                ss << *mi << endl;
        }
        output.write_all_or_retry(ss.str());
        // TODO: print
        //printer(summary);
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
            query.data_start_hook = maker.data_start_hook;
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

std::string ProcessorMaker::verify_option_consistency()
{
	// Check conflicts among options
#ifdef HAVE_LUA
	if (!report.empty())
	{
		if (yaml)
			return "--dump/--yaml conflicts with --report";
		if (json)
			return "--json conflicts with --report";
		if (annotate)
			return "--annotate conflicts with --report";
		//if (summary->boolValue())
		//	return "--summary conflicts with --report";
		if (data_inline)
			return "--inline conflicts with --report";
		if (!postprocess.empty())
			return "--postprocess conflicts with --report";
		if (!sort.empty())
			return "--sort conflicts with --report";
	}
#endif
	if (yaml)
	{
		if (json)
			return "--dump/--yaml conflicts with --json";
		if (data_inline)
			return "--dump/--yaml conflicts with --inline";
		if (data_only)
			return "--dump/--yaml conflicts with --data";
		if (!postprocess.empty())
			return "--dump/--yaml conflicts with --postprocess";
	}
	if (annotate)
	{
		if (data_inline)
			return "--annotate conflicts with --inline";
		if (data_only)
			return "--annotate conflicts with --data";
		if (!postprocess.empty())
			return "--annotate conflicts with --postprocess";
	}
	if (summary)
	{
		if (data_inline)
			return "--summary conflicts with --inline";
		if (data_only)
			return "--summary conflicts with --data";
        if (summary_short)
            return "--summary conflicts with --summary-short";
		if (!postprocess.empty())
			return "--summary conflicts with --postprocess";
		if (!sort.empty())
			return "--summary conflicts with --sort";
	} else if (!summary_restrict.empty())
		return "--summary-restrict only makes sense with --summary";
    if (summary_short)
    {
        if (data_inline)
            return "--summary-short conflicts with --inline";
        if (data_only)
            return "--summary-short conflicts with --data";
        if (!postprocess.empty())
            return "--summary-short conflicts with --postprocess";
        if (!sort.empty())
            return "--summary-short conflicts with --sort";
    }
	if (data_inline)
	{
		if (data_only)
			return "--inline conflicts with --data";
		if (!postprocess.empty())
			return "--inline conflicts with --postprocess";
	}
	if (!postprocess.empty())
	{
		if (data_only)
			return "--postprocess conflicts with --data";
	}
    return std::string();
}

}
}
