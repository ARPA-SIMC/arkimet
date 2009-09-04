/*
 * postprocess - postprocessing of result data
 *
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "postprocess.h"
#include <arki/configfile.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <wibble/operators.h>
#include <wibble/sys/childprocess.h>
#include <wibble/sys/process.h>
#include <wibble/sys/fs.h>
#include <wibble/stream/posix.h>
#include "config.h"
#include <signal.h>
#include <unistd.h>
#include <iostream>

using namespace std;
using namespace wibble;

namespace arki {

namespace postproc {
class Subcommand : public wibble::sys::ChildProcess
{
	vector<string> args;
	int outfd;

	virtual int main()
	{
		try {
			// Redirect stdout to outfd
			if (dup2(outfd, STDOUT_FILENO) < 0)
			{
				//cerr << "OUTFD " << outfd << endl;
				throw wibble::exception::System("redirecting output of postprocessing filter");
			}

			//cerr << "RUN args: " << str::join(args.begin(), args.end()) << endl;

			// Build the argument list
			string basename = str::basename(args[0]);
			const char *argv[args.size()+1];
			for (size_t i = 0; i < args.size(); ++i)
			{
				if (i == 0)
					argv[i] = basename.c_str();
				else
					argv[i] = args[i].c_str();
			}
			argv[args.size()] = 0;

			// Exec the filter
			//cerr << "RUN " << args[0] << " args: " << str::join(&argv[0], &argv[args.size()]) << endl;
			execv(args[0].c_str(), (char* const*)argv);
			throw wibble::exception::System("running postprocessing filter");
		} catch (std::exception& e) {
			cerr << e.what() << endl;
			return 1;
		}
		return 2;
	}

public:
	Subcommand(const vector<string>& args, int outfd)
		: args(args), outfd(outfd)
	{
	}
};

}

void Postprocess::init(const ConfigFile* cfg)
{
	using namespace wibble::operators;
	Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);

	// Build the set of allowed postprocessors
	set<string> allowed;
	if (cfg)
	{
		bool first = true;
		for (ConfigFile::const_section_iterator i = cfg->sectionBegin();
				i != cfg->sectionEnd(); ++i)
		{
			set<string> allowedHere;
			string pp = i->second->value("postprocess");
			for (Splitter::const_iterator j = sp.begin(pp); j != sp.end(); ++j)
				allowedHere.insert(*j);
				
			if (first == true)
			{
				first = false;
				allowed = allowedHere;
			} else {
				allowed &= allowedHere;
			}
		}
	}

	// Parse command into its components
	vector<string> args;
	for (Splitter::const_iterator j = sp.begin(m_command); j != sp.end(); ++j)
		args.push_back(*j);
	//cerr << "Split \"" << m_command << "\" into: " << str::join(args.begin(), args.end(), ", ") << endl;

	// Validate the command
	if (args.empty())
		throw wibble::exception::Consistency("initialising postprocessing filter", "postprocess command is empty");
	if (cfg && allowed.find(args[0]) == allowed.end())
	{
		throw wibble::exception::Consistency("initialising postprocessing filter", "postprocess command " + m_command + " is not supported by all the requested datasets (allowed postprocessors are: " + str::join(allowed.begin(), allowed.end()) + ")");
	}

	// Expand args[0] to the full pathname and check that the program exists

	// Build a list of argv0 candidates
	vector<string> cand_argv0;
	char* env_ppdir = getenv("ARKI_POSTPROC");
	if (env_ppdir)
		cand_argv0.push_back(str::joinpath(env_ppdir, args[0]));
	cand_argv0.push_back(str::joinpath(POSTPROC_DIR, args[0]));

	// Get the first good one from the list
	string argv0;
	for (vector<string>::const_iterator i = cand_argv0.begin();
			i != cand_argv0.end(); ++i)
		if (sys::fs::access(*i, X_OK))
			argv0 = *i;

	if (argv0.empty())
		throw wibble::exception::Consistency("running postprocessing filter", "postprocess command \"" + m_command + "\" does not exists or it is not executable; tried: " + str::join(cand_argv0.begin(), cand_argv0.end()));
	args[0] = argv0;

	// Spawn the command
	m_child = new postproc::Subcommand(args, m_outfd);
	m_child->forkAndRedirect(&m_infd);
}

Postprocess::Postprocess(const std::string& command, int outfd)
	: m_child(0), m_command(command), m_infd(-1), m_outfd(outfd)
{
	init();
}

Postprocess::Postprocess(const std::string& command, int outfd, const ConfigFile& cfg)
	: m_child(0), m_command(command), m_infd(-1), m_outfd(outfd)
{
	init(&cfg);
}

Postprocess::Postprocess(const std::string& command, std::ostream& out)
	: m_child(0), m_command(command), m_infd(1), m_outfd(-1)
{
	stream::PosixBuf* ps = dynamic_cast<stream::PosixBuf*>(out.rdbuf());
	if (!ps)
		throw wibble::exception::Consistency("starting postprocessor", "cannot get a posix file descriptor out of an ostream.  This is a programming error");
	m_outfd = ps->fd();

	init();
}

Postprocess::Postprocess(const std::string& command, std::ostream& out, const ConfigFile& cfg)
	: m_child(0), m_command(command), m_infd(-1), m_outfd(-1)
{
	stream::PosixBuf* ps = dynamic_cast<stream::PosixBuf*>(out.rdbuf());
	if (!ps)
		throw wibble::exception::Consistency("starting postprocessor", "cannot get a posix file descriptor out of an ostream.  This is a programming error");
	m_outfd = ps->fd();

	init(&cfg);
}

Postprocess::~Postprocess()
{
	if (m_infd != 0)
		close(m_infd);

	// If the child still exists, it means that we have not flushed: be
	// aggressive here to cleanup after whatever error condition has happened
	// in the caller
	if (m_child)
	{
		m_child->kill(SIGTERM);
		m_child->wait();
		delete m_child;
	}
}

struct Sigignore
{
	int signum;
	sighandler_t oldsig;
	
	Sigignore(int signum) : signum(signum)
	{
		oldsig = signal(signum, SIG_IGN);
	}
	~Sigignore()
	{
		signal(signum, oldsig);
	}
};

bool Postprocess::operator()(Metadata& md)
{
	if (m_infd == -1)
		return false;
	md.makeInline();
	Sigignore sigignore(SIGPIPE);
	md.write(m_infd, "postprocessing filter");
	return true;
}

void Postprocess::flush()
{
	if (m_infd != -1)
	{
		//cerr << "Closing pipe" << endl;
		if (close(m_infd) < 0)
			throw wibble::exception::System("closing output to postprocessing filter");
		m_infd = -1;
	}
	if (m_child)
	{
		//cerr << "Waiting for child" << endl;
		int res = m_child->wait();
		//cerr << "Waited and got " << res << endl;
		delete m_child;
		m_child = 0;
		if (res)
			throw wibble::exception::Consistency("running postprocessing filter", "postprocess command \"" + m_command + "\" " + sys::process::formatStatus(res));
	}
}

}
// vim:set ts=4 sw=4:
