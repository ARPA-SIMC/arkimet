/*
 * DB-ALLe - Archive for punctual meteorological data
 *
 * Copyright (C) 2005--2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License.
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

#include "benchmark.h"

#include <wibble/exception.h>
#include <stdarg.h>
#include <unistd.h>
#include <ostream>

using namespace std;

static Benchmark* _root = 0;
Benchmark* Benchmark::root()
{
	if (!_root)
		_root = new Benchmark("");
	return _root;
}


double Benchmark::tps = sysconf(_SC_CLK_TCK);

void Benchmark::list(ostream& out)
{
	out << fullName() << endl;
	for (std::vector<Benchmark*>::iterator i = children.begin();
			i != children.end(); i++)
		(*i)->list(out);
}

void Benchmark::elapsed(double& user, double& system, double& total)
{
    struct tms curtms;
    struct timeval curtime;
    if (times(&curtms) == -1)
		throw wibble::exception::System("reading timing informations");
	if (gettimeofday(&curtime, NULL) == -1)
		throw wibble::exception::System("reading time informations");

	user = (curtms.tms_utime - lasttms.tms_utime + curtms.tms_cutime - lasttms.tms_cutime)/tps;
	system = (curtms.tms_stime - lasttms.tms_stime + curtms.tms_cstime - lasttms.tms_cstime)/tps;
	total = (double)((curtime.tv_sec - lasttime.tv_sec) * 1000000 + curtime.tv_usec - lasttime.tv_usec) / 1000000.0;
}

void Benchmark::message(const char* fmt, ...)
{
	string fn = fullName();
	if (!fn.empty())
		fprintf(stdout, "%.*s: ", (int)fn.size(), fn.data());

    va_list ap;
    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);

	fprintf(stdout, "\n");
}

void Benchmark::timing(const char* fmt, ...)
{
	double user, system, total;
	elapsed(user, system, total);

	string fn = fullName();
	if (!fn.empty())
		fprintf(stdout, "%.*s: ", (int)fn.size(), fn.data());

    va_list ap;
    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);

    fprintf(stdout, ": %.2f user, %.2f system, %.2f total\n", user, system, total);

    if (times(&lasttms) == -1)
		throw wibble::exception::System("reading timing informations");
	if (gettimeofday(&lasttime, NULL) == -1)
		throw wibble::exception::System("reading time informations");
}

// Run only the subtest at the given path
void Benchmark::run(const std::string& path)
{
	size_t split = path.find('/');
	if (split == std::string::npos)
	{
		for (std::vector<Benchmark*>::iterator i = children.begin();
				i != children.end(); i++)
			if ((*i)->name() == path)
				return (*i)->run();
	} else {
		std::string child(path, 0, split);
		std::string nextpath(path, split+1);

		for (std::vector<Benchmark*>::iterator i = children.begin();
				i != children.end(); i++)
			if ((*i)->name() == child)
				return (*i)->run(nextpath);
	}
	throw wibble::exception::Consistency("looking for child " + path + " at " + fullName(), "element not found");
}

// Run all subtests and this test
void Benchmark::run()
{
	if (times(&lasttms) == -1)
		throw wibble::exception::System("reading timing informations");
	if (gettimeofday(&lasttime, NULL) == -1)
		throw wibble::exception::System("reading time informations");

	// First, run children
	if (! children.empty())
	{
		for (std::vector<Benchmark*>::iterator i = children.begin();
				i != children.end(); i++)
			(*i)->run();
		timing("total");
	}

	// Then, run self
	main();
}
// vim:set ts=4 sw=4:
