/*
 * arki-xargs - Runs a command on every blob of data found in the input files
 *
 * Copyright (C) 2007--2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <arki/scan/any.h>
#include <arki/runtime.h>

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/exec.h>

#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	VectorOption<String>* inputfiles;
	IntOption* max_args;
	StringOption* max_bytes;
	StringOption* time_interval;

	Options() : StandardParserWithManpage("arki-xargs", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] command [initial-arguments]";
		description =
			"For every item of data read from standard input, save "
			"it on a temporary file and run "
			"'command [initial-arguments] filename' on it";
		inputfiles = add< VectorOption<String> >("input", 'i', "input", "file",
			"read data from this file instead of standard input (can be given more than once)");

		max_args = add<IntOption>("max-args", 'n', "max-args", "count",
			"group at most this amount of data items per command invocation");
		max_bytes = add<StringOption>("max-size", 's', "max-size", "size",
			"create data files no bigger than this size. This may "
			"NOT be respected if there is one single data item "
			"greater than the size specified.  size may be followed "
			"by the following multiplicative suffixes: c=1, w=2, "
			"b=512, kB=1000, K=1024, MB=1000*1000, M=1024*1024, "
			"xM=M GB=1000*1000*1000, G=1024*1024*1024, and so on "
			"for T, P, E, Z, Y");
		time_interval = add<StringOption>("time-interval", 0, "time-interval", "interval",
			"create one data file per 'interval', where interval "
			"can be minute, hour, day, month or year");

		// We need to pass switches in [initial-arguments] untouched
		no_switches_after_first_arg = true;
	}
};

}
}

class Clusterer : public MetadataConsumer
{
protected:
	const vector<string>& args;
	string format;
	size_t count;
	size_t size;
	string tmpfile_name;
	int tmpfile_fd;
	int cur_interval[6];
	types::reftime::Collector timespan;

	void startCluster(const std::string& new_format)
	{
		char fname[] = "arkidata.XXXXXX";
		tmpfile_fd = mkstemp(fname);
		if (tmpfile_fd < 0)
			throw wibble::exception::System("creating temporary file");
		tmpfile_name = fname;
		format = new_format;
		count = 0;
		size = 0;
	}

	void flushCluster()
	{
		if (tmpfile_fd < 0) return;

		// Close the file so that it can be fully read
		if (close(tmpfile_fd) < 0)
		{
			tmpfile_fd = -1;
			throw wibble::exception::File(tmpfile_name, "closing file");
		}
		tmpfile_fd = -1;

		try {
			// Run process with fname as parameter
			run_child();
		} catch (...) {
			// Use unlink and ignore errors, so that we can
			// rethrow the right exception here
			unlink(tmpfile_name.c_str());
			throw;
		} 

		// Delete the file, and tolerate if the child process
		// already deleted it
		sys::fs::deleteIfExists(tmpfile_name);
		format.clear();
		count = 0;
		size = 0;
		cur_interval[0] = -1;
		timespan.clear();
	}

	void run_child()
	{
		if (count == 0) return;

		sys::Exec child(args[0]);
		child.args = args;
		child.args.push_back(tmpfile_name);
		child.searchInPath = true;
		child.importEnv();

		setenv("ARKI_XARGS_FORMAT", str::toupper(format).c_str(), 1);
		setenv("ARKI_XARGS_COUNT", str::fmt(count).c_str(), 1);

		if (timespan.begin.defined())
		{
			setenv("ARKI_XARGS_TIME_START", timespan.begin->toISO8601(' ').c_str(), 1);
			if (timespan.end.defined())
				setenv("ARKI_XARGS_TIME_END", timespan.end->toISO8601(' ').c_str(), 1);
			else
				setenv("ARKI_XARGS_TIME_END", timespan.begin->toISO8601(' ').c_str(), 1);
		}

		child.fork();
		int res = child.wait();
		if (res != 0)
			throw wibble::exception::Consistency("running " + args[0], "process returned exit status " + str::fmt(res));
	}

	void md_to_interval(Metadata& md, int* interval) const
	{
		UItem<types::Reftime> rt = md.get(types::TYPE_REFTIME).upcast<types::Reftime>();
		if (!rt.defined())
			throw wibble::exception::Consistency("computing time interval", "metadata has no reference time");
		UItem<types::Time> t;
		switch (rt->style())
		{
			case types::Reftime::POSITION: t = rt.upcast<types::reftime::Position>()->time; break;
			case types::Reftime::PERIOD: t = rt.upcast<types::reftime::Period>()->end; break;
			default:
				throw wibble::exception::Consistency("computing time interval", "reference time has invalid style: " + types::Reftime::formatStyle(rt->style()));
		}
		for (unsigned i = 0; i < 6; ++i)
			interval[i] = i < max_interval ? t->vals[i] : -1;
	}

	void add_to_cluster(Metadata& md, sys::Buffer& buf)
	{
		ssize_t res = write(tmpfile_fd, buf.data(), buf.size());
		if (res < 0)
			throw wibble::exception::File(tmpfile_name, "writing " + str::fmt(buf.size()));
		if ((size_t)res != buf.size())
			throw wibble::exception::Consistency("writing to " + tmpfile_name, "wrote only " + str::fmt(res) + " bytes out of " + str::fmt(buf.size()));
		++count;
		size += buf.size();
		if (cur_interval[0] == -1 && max_interval != 0)
			md_to_interval(md, cur_interval);
		timespan.merge(md.get(types::TYPE_REFTIME).upcast<types::Reftime>());
	}

	bool exceeds_count(Metadata& md) const
	{
		return (max_args != 0 && count >= max_args);
	}

	bool exceeds_size(sys::Buffer& buf) const
	{
		if (max_bytes == 0 || size == 0) return false;
		return size + buf.size() > max_bytes;
	}

	bool exceeds_interval(Metadata& md) const
	{
		if (max_interval == 0) return false;
		if (cur_interval[0] == -1) return false;
		int candidate[6];
		md_to_interval(md, candidate);
		return memcmp(cur_interval, candidate, 6*sizeof(int)) != 0;
	}

public:
	size_t max_args;
	size_t max_bytes;
	size_t max_interval;

	Clusterer(const vector<string>& args) :
		args(args), tmpfile_fd(-1), max_args(0), max_bytes(0), max_interval(0)
	{
		cur_interval[0] = -1;
	}
	~Clusterer()
	{
		if (tmpfile_fd >= 0)
			flushCluster();
	}

	virtual bool operator()(Metadata& md)
	{
		sys::Buffer buf = md.getData();

		if (format.empty() || format != md.source->format ||
		    exceeds_count(md) || exceeds_size(buf) || exceeds_interval(md))
		{
			flush();
			startCluster(md.source->format);
		}

		add_to_cluster(md, buf);

		return true;
	}

	void flush()
	{
		flushCluster();
	}
};

static void process(Clusterer& consumer, runtime::Input& in)
{
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;

	while (types::readBundle(in.stream(), in.name(), buf, signature, version))
	{
		if (signature != "MD") continue;

		// Read the metadata
		Metadata md;
		md.read(buf, version, in.name());
		if (md.source->style() == types::Source::INLINE)
			md.readInlineData(in.stream(), in.name());
		if (!consumer(md))
			break;
	}
}

static size_t parse_size(const std::string& str)
{
	const char* s = str.c_str();
	char* e;
	unsigned long long int res = strtoull(s, &e, 10);
	string suffix = str.substr(e-s);
	if (suffix.empty() || suffix == "c")
		return res;
	if (suffix == "w") return res * 2;
	if (suffix == "b") return res * 512;
	if (suffix == "kB") return res * 1000;
	if (suffix == "K") return res * 1024;
	if (suffix == "MB") return res * 1000*1000;
	if (suffix == "M" || suffix == "xM") return res * 1024*1024;
	if (suffix == "GB") return res * 1000*1000*1000;
	if (suffix == "G") return res * 1024*1024*1024;
	if (suffix == "TB") return res * 1000*1000*1000*1000;
	if (suffix == "T") return res * 1024*1024*1024*1024;
	if (suffix == "PB") return res * 1000*1000*1000*1000*1000;
	if (suffix == "P") return res * 1024*1024*1024*1024*1024;
	if (suffix == "EB") return res * 1000*1000*1000*1000*1000*1000;
	if (suffix == "E") return res * 1024*1024*1024*1024*1024*1024;
	if (suffix == "ZB") return res * 1000*1000*1000*1000*1000*1000*1000;
	if (suffix == "Z") return res * 1024*1024*1024*1024*1024*1024*1024;
	if (suffix == "YB") return res * 1000*1000*1000*1000*1000*1000*1000*1000;
	if (suffix == "Y") return res * 1024*1024*1024*1024*1024*1024*1024*1024;
	throw wibble::exception::Consistency("parsing size", "unknown suffix: '"+suffix+"'");
}

static size_t parse_interval(const std::string& str)
{
        string name = str::trim(str::tolower(str));
        if (name == "minute") return 5;
        if (name == "hour") return 4;
        if (name == "day") return 3;
        if (name == "month") return 2;
        if (name == "year") return 1;
        throw wibble::exception::Consistency("parsing interval name", "unsupported interval: " + str + ".  Valid intervals are minute, hour, day, month and year");
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		if (!opts.hasNext())
			throw wibble::exception::BadOption("please specify a command to run");

		vector<string> args;
		while (opts.hasNext())
			args.push_back(opts.next());

		runtime::init();

		Clusterer consumer(args);
		if (opts.max_args->isSet())
			consumer.max_args = opts.max_args->intValue();
		if (opts.max_bytes->isSet())
			consumer.max_bytes = parse_size(opts.max_bytes->stringValue());
		if (opts.time_interval->isSet())
			consumer.max_interval = parse_interval(opts.time_interval->stringValue());

		if (opts.inputfiles->values().empty())
		{
			// Process stdin
			runtime::Input in("-");
			process(consumer, in);
			consumer.flush();
		} else {
			// Process the files
			for (vector<string>::const_iterator i = opts.inputfiles->values().begin();
					i != opts.inputfiles->values().end(); ++i)
			{
				runtime::Input in(i->c_str());
				process(consumer, in);
				consumer.flush();
			}
		}

		return 0;
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}

// vim:set ts=4 sw=4:
