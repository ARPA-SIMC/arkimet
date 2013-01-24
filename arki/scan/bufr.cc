/*
 * scan/bufr - Scan a BUFR file for metadata.
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/scan/bufr.h>
#include <arki/utils/files.h>
#include <dballe/msg/repinfo.h>
#include <dballe/msg/msgs.h>
#include <dballe/msg/codec.h>
#include <dballe/core/csv.h>
#include <dballe/core/file.h>
#include <wreport/bulletin.h>
#include <wreport/options.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/reftime.h>
#include <arki/types/run.h>
#include <arki/scan/any.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <sstream>
#include <cstring>
#include <stdint.h>
#include <arpa/inet.h>

#ifdef HAVE_LUA
#include <arki/scan/bufrlua.h>
#endif

using namespace std;
using namespace wibble;
using namespace wreport;
using namespace dballe;

namespace arki {
namespace scan {

namespace bufr {

struct BufrValidator : public Validator
{
	virtual ~BufrValidator() {}

	// Validate data found in a file
	virtual void validate(int fd, off64_t offset, size_t size, const std::string& fname) const
	{
		char buf[4];
		ssize_t res;
		if (size < 8)
			throw wibble::exception::Consistency("checking BUFR segment in file " + fname, "buffer is shorter than 8 bytes");
		if ((res = pread(fd, buf, 4, offset)) == -1)
			throw wibble::exception::System("reading 4 bytes of BUFR header from " + fname);
		if (res != 4)
			throw wibble::exception::Consistency("reading 4 bytes of BUFR header from " + fname, "partial read");
		if (memcmp(buf, "BUFR", 4) != 0)
			throw wibble::exception::Consistency("checking BUFR segment in file " + fname, "segment does not start with 'BUFR'");
		if ((res = pread(fd, buf, 4, offset + size - 4)) == -1)
			throw wibble::exception::System("reading 4 bytes of BUFR trailer from " + fname);
		if (res != 4)
			throw wibble::exception::Consistency("reading 4 bytes of BUFR trailer from " + fname, "partial read");
		if (memcmp(buf, "7777", 4) != 0)
			throw wibble::exception::Consistency("checking BUFR segment in file " + fname, "segment does not end with 'BUFR'");
	}

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const
	{
		if (size < 8)
			throw wibble::exception::Consistency("checking BUFR buffer", "buffer is shorter than 8 bytes");
		if (memcmp(buf, "BUFR", 4) != 0)
			throw wibble::exception::Consistency("checking BUFR buffer", "buffer does not start with 'BUFR'");
		if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
			throw wibble::exception::Consistency("checking BUFR buffer", "buffer does not end with '7777'");
	}
};

static BufrValidator bufr_validator;

const Validator& validator() { return bufr_validator; }

}


Bufr::Bufr(bool inlineData) : file(0), importer(0), m_inline_data(inlineData), extras(0)
{
	msg::Importer::Options opts;
	opts.simplified = true;
	importer = msg::Importer::create(BUFR, opts).release();

	to_rep_memo = read_map_to_rep_memo();
#ifdef HAVE_LUA
	extras = new bufr::BufrLua;
#endif
}

Bufr::~Bufr()
{
	if (importer) delete importer;
	if (file) delete file;
#ifdef HAVE_LUA
	if (extras) delete extras;
#endif
}

void Bufr::open(const std::string& filename)
{
    string basedir, relname;
    utils::files::resolve_path(filename, basedir, relname);
    open(sys::fs::abspath(filename), basedir, relname);
}

void Bufr::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
    // Close the previous file if needed
    close();
    this->filename = filename;
    this->basedir = basedir;
    this->relname = relname;
    file = File::create(BUFR, filename.c_str(), "r").release();
}

void Bufr::close()
{
    filename.clear();
    basedir.clear();
    relname.clear();
    if (file)
    {
        delete file;
        file = 0;
    }
}

static void extract_reftime(const dballe::Msg& msg, Metadata& md)
{
	static int srcs[] = { DBA_MSG_YEAR, DBA_MSG_MONTH, DBA_MSG_DAY, DBA_MSG_HOUR, DBA_MSG_MINUTE, DBA_MSG_SECOND };
	int vals[6];

	for (unsigned i = 0; i < 6; ++i)
	{
		if (const wreport::Var* var = msg.find_by_id(srcs[i]))
			if (var->isset())
			{
				vals[i] = var->enqi();
				continue;
			}
		if (i == 5)
			// In case of seconds, if no value is found we default to 0
			vals[i] = 0;
		else
			return;
	}

	// If we got here, vals is complete
	md.set(types::reftime::Position::create(types::Time::create(vals)));
}

bool Bufr::do_scan(Metadata& md)
{
	Rawmsg rmsg;
	if (!file->read(rmsg))
		return false;

	auto_ptr<BufrBulletin> bulletin(BufrBulletin::create());
	bulletin->decode_header(rmsg, rmsg.file.c_str(), rmsg.offset);

	md.create();

#if 0
	// Detect optional section and handle it
	switch (msg->opt.bufr.optional_section_length)
	{
		case 14: read_info_fixed(msg->opt.bufr.optional_section, md); break;
		case 11: read_info_mobile(msg->opt.bufr.optional_section, md); break;
		default: break;
	}
#endif

	// Set source
	if (m_inline_data)
		md.setInlineData("bufr", wibble::sys::Buffer(rmsg.data(), rmsg.size()));
	else {
		md.source = types::source::Blob::create("bufr", basedir, relname, rmsg.offset, rmsg.size());
		md.setCachedData(wibble::sys::Buffer(rmsg.data(), rmsg.size()));
	}

	// Set reference time
	// FIXME: WRONG! The header date should ALWAYS be ignored
	md.set(types::reftime::Position::create(new types::Time(
			bulletin->rep_year, bulletin->rep_month, bulletin->rep_day,
			bulletin->rep_hour, bulletin->rep_minute, bulletin->rep_second)));

	// Set origin from the bufr header
	UItem<types::product::BUFR> product;
	switch (bulletin->edition)
	{
		case 2:
		case 3:
		case 4:
			// No process?
			md.set(types::origin::BUFR::create(bulletin->centre, bulletin->subcentre));
			md.set(product = types::product::BUFR::create(bulletin->type, bulletin->subtype, bulletin->localsubtype));
			break;
		default:
		{
			std::stringstream str;
			str << "edition is " << bulletin->edition << " but I can only handle 3 and 4";
			throw wibble::exception::Consistency("extracting metadata from BUFR message", str.str());
		}
	}

	// Default to a generic product unless we find more info later
	//md.set(types::product::BUFR::create("generic"));

#ifdef HAVE_LUA
	// If we don't have extra scanning support, we are done
	if (!extras) return true;

    // Try to decode the data; if we fail, we are done
    try {
        // Ignore domain errors, it's ok if we lose some oddball data
        wreport::options::LocalOverride<bool> o(wreport::options::var_silent_domain_errors, true);

        bulletin->decode(rmsg);
    } catch (wreport::error& e) {
        // We can still try to handle partially decoded files

        // Not if the error was not a parse error
        if (e.code() != WR_ERR_PARSE) return true;

        // Not if we didn't decode just one subset
        if (bulletin->subsets.size() != 1) return true;

        // Not if the subset is empty
        if (bulletin->subsets[0].empty()) return true;
    } catch (std::exception& e) {
        return true;
    }

    // If there is more than one subset, we are done
    if (bulletin->subsets.size() != 1)
        return true;

    // Try to parse as a dba_msg
    Msgs msgs;
    try {
        // Ignore domain errors, it's ok if we lose some oddball data
        wreport::options::LocalOverride<bool> o(wreport::options::var_silent_domain_errors, true);

        importer->from_bulletin(*bulletin, msgs);
    } catch (std::exception& e) {
        // If we cannot import it as a Msgs, we are done
        return true;
    }

	// We decoded a different number of subsets than declared by the BUFR.
	// How could this happen? Treat it as an obscure BUFR and go no further
	if (msgs.size() != 1)
		return true;

	// Set the product from the msg type
	ValueBag newvals;
	newvals.set("t", Value::createString(msg_type_name(msgs[0]->type)));
        md.set(product->addValues(newvals));

	// Set reference time from date and time if available
	extract_reftime(*msgs[0], md);

	// DB-All.e managed to make sense of the message: hand it down to Lua
	// to extract further metadata
	extras->scan(*msgs[0], md);
#endif

	return true;
}

bool Bufr::next(Metadata& md)
{
    bool res = do_scan(md);
    if (res)
    {
        // Validate reftime
        Item<types::reftime::Position> rt = md.get<types::reftime::Position>();
        if (rt->time->vals[1] == 0)
            throw wibble::exception::Consistency(
                    "checking reftime " + str::fmt(rt),
                    "month cannot be 0");
        if (rt->time->vals[2] == 0)
            throw wibble::exception::Consistency(
                    "checking reftime " + str::fmt(rt),
                    "day cannot be 0");
    }
    return res;
}

namespace {
	struct fill_to_repmemo
	{
		std::map<int, std::string>& dest;
		fill_to_repmemo(std::map<int, std::string>& dest) : dest(dest) {}
		void operator()(int cod, const std::string& memo)
		{
			dest.insert(make_pair(cod, memo));
		}
	};
	struct fill_to_repcod
	{
		std::map<std::string, int>& dest;
		fill_to_repcod(std::map<std::string, int>& dest) : dest(dest) {}
		void operator()(int cod, const std::string& memo)
		{
			dest.insert(make_pair(memo, cod));
		}
	};
}

static inline void inplace_tolower(std::string& buf)
{
	for (std::string::iterator i = buf.begin(); i != buf.end(); ++i)
		*i = tolower(*i);
}

template<typename T>
static void read_repinfo(const std::string& fname, T& consumer)
{
	FILE* in;
	const char* name;

	if (fname.empty())
		name = repinfo_default_filename();
	else
		name = fname.c_str();


	// Open the input CSV file
	in = fopen(name, "r");
	if (in == NULL)
		throw wibble::exception::File(name, "opening file");

	// Close it on exit
	struct HandleCloser {
		FILE* fd;
		HandleCloser(FILE* fd) : fd(fd) {}
		~HandleCloser() { fclose(fd); }
	} closer(in);

	// Read the CSV file
	vector<string> columns;
	for (int line = 0; csv_read_next(in, columns); ++line)
	{
		int id;

		if (columns.size() != 6)
			error_parse::throwf(name, line, "Expected 6 columns, got %zd", columns.size());

		// Lowercase all rep_memos
		inplace_tolower(columns[1]);
		id = strtol(columns[0].c_str(), 0, 10);

		// Hand it to consumer
		consumer(id, columns[1]);
	}
}

std::map<std::string, int> Bufr::read_map_to_rep_cod(const std::string& fname)
{
	std::map<std::string, int> res;
	fill_to_repcod consumer(res);
	read_repinfo(fname, consumer);
	return res;
}

std::map<int, std::string> Bufr::read_map_to_rep_memo(const std::string& fname)
{
	std::map<int, std::string> res;
	fill_to_repmemo consumer(res);
	read_repinfo(fname, consumer);
	return res;
}

int Bufr::update_sequence_number(const std::string& buf)
{
    auto_ptr<BufrBulletin> bulletin(BufrBulletin::create());
    bulletin->decode_header(buf);
    return bulletin->update_sequence_number;
}

}
}
// vim:set ts=4 sw=4:
