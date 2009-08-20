/*
 * dataset/targetfile - Generator for target file names for data in the dataset
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "targetfile.h"

#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/types/reftime.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/grcal/grcal.h>

#include <sstream>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace wibble;
namespace rt = arki::types::reftime;
namespace gd = wibble::grcal::date;

namespace arki {
namespace dataset {

struct BaseTargetFile : public TargetFile
{
	virtual UItem<types::Reftime> reftimeForPath(const std::string& path) const = 0;

    virtual bool pathMatches(const std::string& path, const matcher::Implementation& m) const
	{
		UItem<types::Reftime> rt = reftimeForPath(path);
		if (!rt.defined()) return false;
		return m.matchItem(rt);
	}
};

struct Yearly : public BaseTargetFile
{
	static const char* name() { return "yearly"; }

	UItem<types::Reftime> reftimeForPath(const std::string& path) const
	{
		int dummy;
		int base[6] = { -1, -1, -1, -1, -1, -1 };
		int min[6];
		int max[6];
		if (sscanf(path.c_str(), "%02d/%04d", &dummy, &base[0]) != 2)
			return UItem<types::Reftime>();

		gd::lowerbound(base, min);
		gd::upperbound(base, max);
		return rt::Period::create(types::Time::create(min), types::Time::create(max));
	}
	
	std::string operator()(const Metadata& md)
	{
		UItem<types::Time> tt = md.get(types::TYPE_REFTIME).upcast<types::reftime::Position>()->time;
		char buf[9];
		snprintf(buf, 9, "%02d/%04d", (*tt)[0]/100, (*tt)[0]);
		return buf;
	}
};

struct Monthly : public BaseTargetFile
{
	static const char* name() { return "monthly"; }

	UItem<types::Reftime> reftimeForPath(const std::string& path) const
	{
		int base[6] = { -1, -1, -1, -1, -1, -1 };
		int min[6];
		int max[6];
		if (sscanf(path.c_str(), "%04d/%02d", &base[0], &base[1]) == 0)
			return UItem<types::Reftime>();

		gd::lowerbound(base, min);
		gd::upperbound(base, max);
		return rt::Period::create(types::Time::create(min), types::Time::create(max));
	}
	
	std::string operator()(const Metadata& md)
	{
		UItem<types::Time> tt = md.get(types::TYPE_REFTIME).upcast<types::reftime::Position>()->time;
		char buf[10];
		snprintf(buf, 10, "%04d/%02d", (*tt)[0], (*tt)[1]);
		return buf;
	}
};

struct Biweekly : public BaseTargetFile
{
	static const char* name() { return "biweekly"; }

	UItem<types::Reftime> reftimeForPath(const std::string& path) const
	{
		int year, month = -1, biweek = -1;
		int min[6] = { -1, -1, -1, -1, -1, -1 };
		int max[6] = { -1, -1, -1, -1, -1, -1 };
		if (sscanf(path.c_str(), "%04d/%02d-%d", &year, &month, &biweek) == 0)
			return UItem<types::Reftime>();
		min[0] = max[0] = year;
		min[1] = max[1] = month;
		switch (biweek)
		{
			case 1: min[2] = 1; max[2] = 14; break;
			case 2: min[2] = 15; max[2] = -1; break;
			default: break;
		}
		gd::lowerbound(min);
		gd::upperbound(max);
		return rt::Period::create(types::Time::create(min), types::Time::create(max));
	}
	
	std::string operator()(const Metadata& md)
	{
		UItem<types::Time> tt = md.get(types::TYPE_REFTIME).upcast<types::reftime::Position>()->time;
		char buf[10];
		snprintf(buf, 10, "%04d/%02d-", (*tt)[0], (*tt)[1]);
		stringstream res;
		res << buf;
		res << ((*tt)[2] > 15 ? 2 : 1);
		return res.str();
	}
};

struct Weekly : public BaseTargetFile
{
	static const char* name() { return "weekly"; }

	UItem<types::Reftime> reftimeForPath(const std::string& path) const
	{
		int year, month = -1, week = -1;
		int min[6] = { -1, -1, -1, -1, -1, -1 };
		int max[6] = { -1, -1, -1, -1, -1, -1 };
		if (sscanf(path.c_str(), "%04d/%02d-%d", &year, &month, &week) == 0)
			return UItem<types::Reftime>();
		min[0] = max[0] = year;
		min[1] = max[1] = month;
		if (week != -1)
		{
			min[2] = (week - 1) * 7 + 1;
			max[2] = min[2] + 6;
		}
		gd::lowerbound(min);
		gd::upperbound(max);
		return rt::Period::create(types::Time::create(min), types::Time::create(max));
	}
	
	std::string operator()(const Metadata& md)
	{
		UItem<types::Time> tt = md.get(types::TYPE_REFTIME).upcast<types::reftime::Position>()->time;
		char buf[10];
		snprintf(buf, 10, "%04d/%02d-", (*tt)[0], (*tt)[1]);
		stringstream res;
		res << buf;
		res << ((((*tt)[2]-1) / 7) + 1);
		return res.str();
	}
};

struct Daily : public BaseTargetFile
{
	static const char* name() { return "daily"; }

	UItem<types::Reftime> reftimeForPath(const std::string& path) const
	{
		int base[6] = { -1, -1, -1, -1, -1, -1 };
		int min[6];
		int max[6];
		if (sscanf(path.c_str(), "%04d/%02d-%02d", &base[0], &base[1], &base[2]) == 0)
			return UItem<types::Reftime>();

		gd::lowerbound(base, min);
		gd::upperbound(base, max);
		return rt::Period::create(types::Time::create(min), types::Time::create(max));
	}
	
	std::string operator()(const Metadata& md)
	{
		UItem<types::Time> tt = md.get(types::TYPE_REFTIME).upcast<types::reftime::Position>()->time;
		char buf[15];
		snprintf(buf, 15, "%04d/%02d-%02d", (*tt)[0], (*tt)[1], (*tt)[2]);
		return buf;
	}
};

TargetFile* TargetFile::create(const ConfigFile& cfg)
{
    string step = wibble::str::tolower(cfg.value("step"));

	if (step == Daily::name())
		return new Daily;
	else if (step == Weekly::name())
		return new Weekly;
	else if (step == Biweekly::name())
		return new Biweekly;
	else if (step == Monthly::name())
		return new Monthly;
	else if (step == Yearly::name())
		return new Yearly;
	else
		throw wibble::exception::Consistency(
			"step '"+step+"' is not supported.  Valid values are daily, weekly, biweekly, monthly and yearly.",
			"parsing dataset time step");
}

std::vector<std::string> TargetFile::stepList()
{
	vector<string> res;
	res.push_back(Daily::name());
	res.push_back(Weekly::name());
	res.push_back(Biweekly::name());
	res.push_back(Monthly::name());
	res.push_back(Yearly::name());
	return res;
}

}
}
// vim:set ts=4 sw=4:
