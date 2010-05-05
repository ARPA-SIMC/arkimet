#ifndef ARKI_REPORT_H
#define ARKI_REPORT_H

/*
 * arki/report - Build a report of an arkimet metadata or summary stream
 *
 * Copyright (C) 2008--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/consumer.h>
#include <string>
#include <iosfwd>

namespace arki {
struct Lua;
struct Summary;

class Report : public metadata::Consumer
{
protected:
	mutable Lua *L;
	std::string m_filename;
	bool m_accepts_metadata;
	bool m_accepts_summary;

	void create_lua_object();

public:
	Report(const std::string& params = std::string());
	virtual ~Report();

	/// Return true if this report can process metadata
	bool acceptsMetadata() const { return m_accepts_metadata; }

	/// Return true if this report can process summaries
	bool acceptsSummary() const { return m_accepts_summary; }

	/// Load the report described by the given string
	void load(const std::string& params);

	/// Load the report code from the given file
	void loadFile(const std::string& fname);
	/// Load the report code from the given string containing Lua source code
	void loadString(const std::string& buf);

	/// Send Lua's print output to an ostream
	void captureOutput(std::ostream& buf);

	/// Process a metadata for the report
	virtual bool operator()(Metadata& md);

	/// Process a summary for the report
	virtual bool operator()(Summary& s);

	/// Done sending input: produce the report
	void report();
};

}

// vim:set ts=4 sw=4:
#endif
