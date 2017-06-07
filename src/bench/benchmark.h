/*
 * DB-ALLe - Archive for punctual meteorological data
 *
 * Copyright (C) 2005,2006  ARPAE-SIMC <simc-urp@arpae.it>
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

#ifndef ARKI_BENCH_BENCHMARK_H
#define ARKI_BENCH_BENCHMARK_H

#include <sys/times.h>
#include <sys/time.h>

#include <vector>
#include <string>
#include <iosfwd>

class Benchmark
{
private:
	static double tps;
	std::string tag;
	struct tms lasttms;
	struct timeval lasttime;
	Benchmark* parent;
	std::vector<Benchmark*> children;
	
protected:
	// Main function with the benchmarks
	virtual void main() {}

	// Print timing information since the last timing call
	void timing(const char* fmt, ...);

	// Print a message, without timing information
	void message(const char* fmt, ...);

	// Get the floating point number of seconds passed since the last timing
	// call
	void elapsed(double& user, double& system, double& total);

	void setParent(Benchmark* parent) { this->parent = parent; }

	std::string name() const { return tag; }

	std::string fullName() const
	{
		if (parent)
			return parent->fullName() + "/" + tag;
		else
			return tag;
	}
	
public:
	Benchmark(const std::string& tag) : tag(tag), parent(0) {}

	void addChild(Benchmark* child)
	{
		children.push_back(child);
		child->setParent(this);
	}

	virtual ~Benchmark()
	{
		for (std::vector<Benchmark*>::iterator i = children.begin();
				i != children.end(); i++)
			delete *i;
	}

	void list(std::ostream& out);

	// Run only the subtest at the given path
	void run(const std::string& path);
	
	// Run all subtests and this test
	void run();

	// Return the singleton instance of the toplevel benchmark class
	static Benchmark* root();
};

struct RegisterRoot
{
	RegisterRoot(Benchmark* b)
	{
		Benchmark::root()->addChild(b);
	}
};

// vim:set ts=4 sw=4:
#endif
