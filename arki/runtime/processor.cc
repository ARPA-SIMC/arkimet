/*
 * runtime/processor - Run user requested operations on datasets
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/runtime/processor.h>
#include <arki/runtime/io.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/printer.h>
#include <arki/formatter.h>
#include <arki/dataset.h>
#include <arki/dataset/index/base.h>
#include <arki/targetfile.h>
#include <arki/summary.h>
#include <arki/sort.h>
#include <arki/utils/dataset.h>
#include <wibble/string.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace runtime {

metadata::Printer* createPrinter(ProcessorMaker& maker, arki::Output& out)
{
    if (maker.json)
        return new metadata::JSONPrinter(out, maker.annotate);
    else if (maker.yaml || maker.annotate)
        return new metadata::YamlPrinter(out, maker.annotate);
    else
        return new metadata::BinaryPrinter(out);
}

struct Inliner : public metadata::Eater
{
    metadata::Eater& next;

    Inliner(metadata::Eater& next) : next(next) {}

    bool eat(auto_ptr<Metadata> md) override
    {
        md->makeInline();
        return next.eat(md);
    }
};

struct DataProcessor : public DatasetProcessor
{
    arki::Output& output;
    metadata::Printer* printer;
    dataset::DataQuery query;
    vector<string> description_attrs;
    bool data_inline;

    DataProcessor(ProcessorMaker& maker, Matcher& q, arki::Output& out, bool data_inline=false)
        : output(out), printer(createPrinter(maker, out)), query(q), data_inline(data_inline)
    {
        description_attrs.push_back("query=" + q.toString());
        description_attrs.push_back("printer=" + printer->describe());
        if (data_inline)
            description_attrs.push_back("data_inline=true");
        if (!maker.sort.empty())
        {
            description_attrs.push_back("sort=" + maker.sort);
            query.sorter = sort::Compare::parse(maker.sort);
        }
    }

    virtual ~DataProcessor()
    {
        if (printer) delete printer;
    }

    virtual std::string describe() const
    {
        string res = "data(";
        res += str::join(description_attrs.begin(), description_attrs.end(), ", ");
        res += ")";
        return res;
    }

    virtual void process(ReadonlyDataset& ds, const std::string& name)
    {
        if (data_inline)
        {
            Inliner inliner(*printer);
            ds.queryData(query, inliner);
        } else {
            utils::ds::MakeAbsolute absoluter(*printer);
            ds.queryData(query, absoluter);
        }
    }

    virtual void end()
    {
        output.stream().flush();
    }
};

struct SummaryProcessor : public DatasetProcessor
{
    arki::Output& output;
    Matcher matcher;
    metadata::Printer* printer;
    string summary_restrict;
    Summary summary;
    vector<string> description_attrs;

    SummaryProcessor(ProcessorMaker& maker, Matcher& q, arki::Output& out)
        : output(out), matcher(q), printer(createPrinter(maker, out))
    {
        description_attrs.push_back("query=" + q.toString());
        if (!maker.summary_restrict.empty())
            description_attrs.push_back("restrict=" + maker.summary_restrict);
        description_attrs.push_back("printer=" + printer->describe());
        summary_restrict = maker.summary_restrict;
    }

    virtual ~SummaryProcessor()
    {
        if (printer) delete printer;
    }

    virtual std::string describe() const
    {
        string res = "summary(";
        res += str::join(description_attrs.begin(), description_attrs.end(), ", ");
        res += ")";
        return res;
    }

    virtual void process(ReadonlyDataset& ds, const std::string& name)
    {
        ds.querySummary(matcher, summary);
    }

    virtual void end()
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
        printer->observe_summary(s);
        output.stream().flush();
    }
};


struct BinaryProcessor : public DatasetProcessor
{
    arki::Output& out;
	dataset::ByteQuery query;
    vector<string> description_attrs;

	BinaryProcessor(ProcessorMaker& maker, Matcher& q, arki::Output& out)
		: out(out)
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

	virtual ~BinaryProcessor()
	{
	}

    virtual std::string describe() const
    {
        string res = "binary(";
        res += str::join(description_attrs.begin(), description_attrs.end(), ", ");
        res += ")";
        return res;
    }

	virtual void process(ReadonlyDataset& ds, const std::string& name)
	{
		// TODO: validate query's postprocessor with ds' config
		ds.queryBytes(query, out.stream());
	}

    virtual void end()
    {
        out.stream().flush();
    }
};


TargetFileProcessor::TargetFileProcessor(DatasetProcessor* next, const std::string& pattern, arki::Output& output)
		: next(next), pattern(pattern), output(output)
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
    res += str::join(description_attrs.begin(), description_attrs.end(), ", ");
    res += ")";
    return res;
}

void TargetFileProcessor::process(ReadonlyDataset& ds, const std::string& name)
{
    if (runtime::Output* out = dynamic_cast<runtime::Output*>(&output))
    {
        TargetfileSpy spy(ds, *out, pattern);
        next->process(spy, name);
    } else {
        throw wibble::exception::Consistency("setting up targetfile", "programming error: output is not a runtime::Output");
    }
}

void TargetFileProcessor::end() { next->end(); }


std::auto_ptr<DatasetProcessor> ProcessorMaker::make(Matcher query, arki::Output& out)
{
    if (data_only || !postprocess.empty()
#ifdef HAVE_LUA
        || !report.empty()
#endif
        )
        return auto_ptr<DatasetProcessor>(new BinaryProcessor(*this, query, out));
    else if (summary)
        return auto_ptr<DatasetProcessor>(new SummaryProcessor(*this, query, out));
    else
        return auto_ptr<DatasetProcessor>(new DataProcessor(*this, query, out, data_inline));
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
		if (!postprocess.empty())
			return "--summary conflicts with --postprocess";
		if (!sort.empty())
			return "--summary conflicts with --sort";
	} else if (!summary_restrict.empty())
		return "--summary-restrict only makes sense with --summary";
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
