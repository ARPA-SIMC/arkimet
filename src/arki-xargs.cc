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
#include <arki/scan/any.h>
#include <arki/runtime.h>

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/exec.h>

#include <cstdlib>
#include <unistd.h>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	wibble::commandline::VectorOption<wibble::commandline::String>* inputfiles;

	Options() : StandardParserWithManpage("arki-xargs", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] command [initial-arguments]";
		description =
			"For every item of data read from standard input, save "
			"it on a temporary file and run "
			"'command [initial-arguments] filename' on it";
		inputfiles = add< VectorOption<String> >("input", 'i', "input", "file",
			"read data from this file instead of standard input (can be given more than once)");

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
	string tmpfile_name;
	int tmpfile_fd;

	void startCluster(const std::string& new_format)
	{
		char fname[] = "arkidata.XXXXXX";
		tmpfile_fd = mkstemp(fname);
		if (tmpfile_fd < 0)
			throw wibble::exception::System("creating temporary file");
		tmpfile_name = fname;
		format = new_format;
		count = 0;
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
	}

	void run_child()
	{
		if (count == 0) return;

		sys::Exec child(args[0]);
		child.args = args;
		child.args.push_back(tmpfile_name);
		child.envFromParent = false;
		child.searchInPath = true;
		child.importEnv();

		child.env.push_back("ARKI_XARGS_FORMAT=" + str::toupper(format));
		child.env.push_back("ARKI_XARGS_COUNT=" + str::fmt(count));

		child.fork();
		int res = child.wait();
		if (res != 0)
			throw wibble::exception::Consistency("running " + args[0], "process returned exit status " + str::fmt(res));
	}

	virtual void add_to_cluster(Metadata& md)
	{
		sys::Buffer buf = md.getData();
		ssize_t res = write(tmpfile_fd, buf.data(), buf.size());
		if (res < 0)
			throw wibble::exception::File(tmpfile_name, "writing " + str::fmt(buf.size()));
		if ((size_t)res != buf.size())
			throw wibble::exception::Consistency("writing to " + tmpfile_name, "wrote only " + str::fmt(res) + " bytes out of " + str::fmt(buf.size()));
		++count;
	}

public:
	Clusterer(const vector<string>& args) : args(args), tmpfile_fd(-1) {}
	~Clusterer()
	{
		if (tmpfile_fd >= 0)
			flushCluster();
	}

	virtual bool operator()(Metadata& md)
	{
		if (format.empty() || format != md.source->format)
		{
			flush();
			startCluster(md.source->format);
		}

		add_to_cluster(md);

		return true;
	}

	void flush()
	{
		flushCluster();
	}
};

static void process(const vector<string>& args, runtime::Input& in)
{
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;

	Clusterer consumer(args);

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

		if (opts.inputfiles->values().empty())
		{
			// Process stdin
			runtime::Input in("-");
			process(args, in);
		} else {
			// Process the files
			for (vector<string>::const_iterator i = opts.inputfiles->values().begin();
					i != opts.inputfiles->values().end(); ++i)
			{
				runtime::Input in(i->c_str());
				process(args, in);
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
