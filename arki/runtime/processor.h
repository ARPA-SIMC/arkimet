#ifndef ARKI_RUNTIME_PROCESSOR_H
#define ARKI_RUNTIME_PROCESSOR_H

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

#if 0
#include <arki/runtime/io.h>
#include <arki/runtime/config.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/configfile.h>
#include <sys/time.h>
#endif
#include <string>
#include <memory>

namespace arki {

class ReadonlyDataset;
class Matcher;
#if 0
class Dataset;
class Summary;
class Dispatcher;
class Formatter;
class Targetfile;
#endif

namespace runtime {
class Output;

struct DatasetProcessor
{
	virtual ~DatasetProcessor() {}

	virtual void process(ReadonlyDataset& ds, const std::string& name) = 0;
	virtual void end() {}
};

struct TargetFileProcessor : public DatasetProcessor
{
    DatasetProcessor* next;
    std::string pattern;
    Output& output;

    TargetFileProcessor(DatasetProcessor* next, const std::string& pattern, Output& output);
    virtual ~TargetFileProcessor();

    virtual void process(ReadonlyDataset& ds, const std::string& name);

    virtual void end();
};

struct ProcessorMaker
{
    bool summary;
    bool yaml;
    bool annotate;
    bool data_only;
    bool data_inline;
    std::string postprocess;
    std::string report;

    std::string summary_restrict;
    std::string sort;

    ProcessorMaker()
        : summary(false), yaml(false), annotate(false), data_only(false), data_inline(false)
    {
    }

    /// Create the processor maker for this configuration
    std::auto_ptr<DatasetProcessor> make(Matcher& query, Output& out);

    /**
     * Consistency check on the maker configuration.
     *
     * @returns the empty string if all is ok, else an error message
     */
    std::string verify_option_consistency();
};


}
}

// vim:set ts=4 sw=4:
#endif
