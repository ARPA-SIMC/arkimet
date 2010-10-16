/*
 * runtime/processor - Run user requested operations on datasets
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/runtime/processor.h>
#include <arki/runtime/io.h>
#include <arki/metadata/consumer.h>
#include <arki/formatter.h>
#include <arki/dataset.h>
#include <arki/dataset/index/base.h>
#include <arki/targetfile.h>
#include <arki/summary.h>
#include <arki/sort.h>

#if 0
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/signal.h>
#include <wibble/string.h>
#include <arki/configfile.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/dataset/file.h>
#include <arki/dataset/http.h>
#include <arki/dispatcher.h>
#include <arki/postprocess.h>
#include <arki/nag.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <cstdlib>
#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif
#endif

using namespace std;

namespace arki {
namespace runtime {

struct YamlProcessor : public DatasetProcessor, public metadata::Consumer
{
    Formatter* formatter;
	Output& output;
	Summary* summary;
	sort::Compare* sorter;
	dataset::DataQuery query;
	string summary_restrict;

	YamlProcessor(ProcessorMaker& maker, Matcher& query, Output& out)
		: formatter(0), output(out), summary(0), sorter(0),
		  query(query, false)
	{
		if (maker.annotate)
			formatter = Formatter::create();

		if (maker.summary)
		{
			summary = new Summary();
            summary_restrict = maker.summary_restrict;
		}
		else if (!maker.sort.empty())
		{
			sorter = sort::Compare::parse(maker.sort).release();
			this->query.sorter = sorter;
		}
	}

	virtual ~YamlProcessor()
	{
		if (sorter) delete sorter;
		if (summary) delete summary;
		if (formatter) delete formatter;
	}

	virtual void process(ReadonlyDataset& ds, const std::string& name)
	{
		if (summary)
			ds.querySummary(query.matcher, *summary);
		else
			ds.queryData(query, *this);
	}

	virtual bool operator()(Metadata& md)
	{
		md.writeYaml(output.stream(), formatter);
		output.stream() << endl;
		return true;
	}

	virtual void end()
	{
		if (summary)
		{
			if (!summary_restrict.empty())
			{
				Summary s;
				s.add(*summary, dataset::index::parseMetadataBitmask(summary_restrict));
				s.writeYaml(output.stream(), formatter);
			} else
				summary->writeYaml(output.stream(), formatter);
			output.stream() << endl;
		}
	}
};

struct BinaryProcessor : public DatasetProcessor
{
	Output& out;
	sort::Compare* sorter;
	dataset::ByteQuery query;

	BinaryProcessor(ProcessorMaker& maker, Matcher& q, Output& out)
		: out(out), sorter(0)
	{
		if (!maker.postprocess.empty())
		{
			query.setPostprocess(q, maker.postprocess);
#ifdef HAVE_LUA
		} else if (!maker.report.empty()) {
			if (maker.summary)
				query.setRepSummary(q, maker.report);
			else
				query.setRepMetadata(q, maker.report);
#endif
		} else
			query.setData(q);
		
		if (!maker.sort.empty())
		{
			sorter = sort::Compare::parse(maker.sort).release();
			query.sorter = sorter;
		}
	}

	virtual ~BinaryProcessor()
	{
		if (sorter) delete sorter;
	}

	virtual void process(ReadonlyDataset& ds, const std::string& name)
	{
		// TODO: validate query's postprocessor with ds' config
		ds.queryBytes(query, out.stream());
	}
};

struct DataProcessor : public DatasetProcessor, public metadata::Consumer
{
	Output& output;
	Summary* summary;
	sort::Compare* sorter;
	dataset::DataQuery query;
	string summary_restrict;

	DataProcessor(ProcessorMaker& maker, Matcher& q, Output& out)
		: output(out), summary(0), sorter(0),
		  query(q, false)
	{
		if (maker.summary)
		{
			summary = new Summary();
            summary_restrict = maker.summary_restrict;
		}
		else
		{
			query.withData = maker.data_inline;

			if (!maker.sort.empty())
			{
				sorter = sort::Compare::parse(maker.sort).release();
				query.sorter = sorter;
			}
		}
	}

	virtual ~DataProcessor()
	{
		if (sorter) delete sorter;
		if (summary) delete summary;
	}

	virtual void process(ReadonlyDataset& ds, const std::string& name)
	{
		if (summary)
			ds.querySummary(query.matcher, *summary);
		else
			ds.queryData(query, *this);
	}

	virtual bool operator()(Metadata& md)
	{
		md.write(output.stream(), output.name());
		return true;
	}

	virtual void end()
	{
		if (summary)
		{
			if (!summary_restrict.empty())
			{
				Summary s;
				s.add(*summary, dataset::index::parseMetadataBitmask(summary_restrict));
				s.write(output.stream(), output.name());
			} else
				summary->write(output.stream(), output.name());
		}
	}
};


TargetFileProcessor::TargetFileProcessor(DatasetProcessor* next, const std::string& pattern, Output& output)
		: next(next), pattern(pattern), output(output)
{
}

TargetFileProcessor::~TargetFileProcessor()
{
		if (next) delete next;
}

void TargetFileProcessor::process(ReadonlyDataset& ds, const std::string& name)
{
		TargetfileSpy spy(ds, output, pattern);
		next->process(spy, name);
}

void TargetFileProcessor::end() { next->end(); }


std::auto_ptr<DatasetProcessor> ProcessorMaker::make(Matcher& query, Output& out)
{
    if (yaml || annotate)
        return auto_ptr<DatasetProcessor>(new YamlProcessor(*this, query, out));
    else if (data_only || !postprocess.empty()
#ifdef HAVE_LUA
        || !report.empty()
#endif
        )
        return auto_ptr<DatasetProcessor>(new BinaryProcessor(*this, query, out));
    else
        return auto_ptr<DatasetProcessor>(new DataProcessor(*this, query, out));
}

std::string ProcessorMaker::verify_option_consistency()
{
	// Check conflicts among options
#ifdef HAVE_LUA
	if (!report.empty())
	{
		if (yaml)
			return "--dump/--yaml conflicts with --report";
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
// vim:set ts=4 sw=4:
