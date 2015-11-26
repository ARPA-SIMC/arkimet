#include "config.h"
#include <arki/scan/bufr.h>
#include <arki/utils/files.h>
#include <dballe/file.h>
#include <dballe/message.h>
#include <dballe/msg/codec.h>
#include <dballe/core/csv.h>
#include <wreport/bulletin.h>
#include <wreport/options.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/reftime.h>
#include <arki/types/run.h>
#include <arki/types/timerange.h>
#include <arki/scan/any.h>
#include <arki/utils/sys.h>
#include <wibble/grcal/grcal.h>
#include <sstream>
#include <cstring>
#include <stdint.h>
#include <arpa/inet.h>

#ifdef HAVE_LUA
#include <arki/scan/bufrlua.h>
#endif

using namespace std;
using namespace wreport;
using namespace dballe;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace scan {

namespace bufr {

struct BufrValidator : public Validator
{
	virtual ~BufrValidator() {}

    // Validate data found in a file
    virtual void validate(int fd, off_t offset, size_t size, const std::string& fname) const
    {
        if (size < 8)
        {
            stringstream ss;
            ss << fname << ":" << offset << ": cannot check BUFR segment: file segment to check is only " << size << " bytes (minimum for a BUFR is 8)";
            throw runtime_error(ss.str());
        }

        sys::NamedFileDescriptor f(fd, fname);
        char buf[4];
        ssize_t res;
        if ((res = f.pread(buf, 4, offset)) != 4)
        {
            stringstream ss;
            ss << fname << ":" << offset << ": could only read " << res << "/4 bytes of BUFR header";
            throw runtime_error(ss.str());
        }
        if (memcmp(buf, "BUFR", 4) != 0)
        {
            stringstream ss;
            ss << fname << ":" << offset << ": segment does not start with 'BUFR'";
            throw runtime_error(ss.str());
        }
        if ((res = f.pread(buf, 4, offset + size - 4)) != 4)
        {
            stringstream ss;
            ss << fname << ":" << offset << ": could only read " << res << "/4 bytes of BUFR trailer";
            throw runtime_error(ss.str());
        }
        if (memcmp(buf, "7777", 4) != 0)
        {
            stringstream ss;
            ss << fname << ":" << offset << ": segment does not end with '7777'";
            throw runtime_error(ss.str());
        }
    }

    // Validate a memory buffer
    virtual void validate(const void* buf, size_t size) const
    {
        if (size < 8)
            throw runtime_error("BUFR buffer is shorter than 8 bytes");
        if (memcmp(buf, "BUFR", 4) != 0)
            throw runtime_error("BUFR buffer does not start with 'BUFR'");
        if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
            throw runtime_error("BUFR buffer does not end with '7777'");
    }
};

static BufrValidator bufr_validator;

const Validator& validator() { return bufr_validator; }

}


Bufr::Bufr() : file(0), importer(0), extras(0)
{
	msg::Importer::Options opts;
	opts.simplified = true;
	importer = msg::Importer::create(File::BUFR, opts).release();

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
    open(sys::abspath(filename), basedir, relname);
}

void Bufr::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
    // Close the previous file if needed
    close();
    this->filename = filename;
    this->basedir = basedir;
    this->relname = relname;
    if (filename == "-")
        file = File::create(File::BUFR, stdin, false, "standard input").release();
    else
        file = File::create(File::BUFR, filename.c_str(), "r").release();
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

namespace {
class Harvest
{
protected:
    Messages msgs;

public:
    dballe::msg::Importer& importer;
    unique_ptr<reftime::Position> reftime;
    unique_ptr<Origin> origin;
    unique_ptr<product::BUFR> product;
    Message* msg;

    Harvest(dballe::msg::Importer& importer) : importer(importer), msg(0) {}

    void refine_reftime(const Message& msg)
    {
        Datetime dt = msg.get_datetime();
        if (dt.is_missing()) return;
        reftime->time = types::Time(
                dt.year, dt.month, dt.day,
                dt.hour, dt.minute, dt.second);
    }

    void harvest_from_dballe(const BinaryMessage& rmsg, Metadata& md)
    {
        msg = 0;
        auto bulletin = BufrBulletin::decode_header(rmsg.data, rmsg.pathname.c_str(), rmsg.offset);

#if 0
        // Detect optional section and handle it
        switch (msg->opt.bufr.optional_section_length)
        {
            case 14: read_info_fixed(msg->opt.bufr.optional_section, md); break;
            case 11: read_info_mobile(msg->opt.bufr.optional_section, md); break;
            default: break;
        }
#endif

        // Set reference time
        // FIXME: WRONG! The header date should ALWAYS be ignored
        reftime = reftime::Position::create(types::Time(
                bulletin->rep_year, bulletin->rep_month, bulletin->rep_day,
                bulletin->rep_hour, bulletin->rep_minute, bulletin->rep_second));

        // Set origin from the bufr header
        switch (bulletin->edition_number)
        {
            case 2:
            case 3:
            case 4:
                // No process?
                origin = Origin::createBUFR(bulletin->originating_centre, bulletin->originating_subcentre);
                product = product::BUFR::create(bulletin->data_category, bulletin->data_subcategory, bulletin->data_subcategory_local);
                break;
            default: {
                std::stringstream ss;
                ss << "cannot extract metadata from BUFR message: edition is " << bulletin->edition_number << " but I can only handle 3 and 4";
                throw std::runtime_error(ss.str());
            }
        }

        // Default to a generic product unless we find more info later
        //md.set(types::product::BUFR::create("generic"));

        // Try to decode the data; if we fail, we are done
        try {
            // Ignore domain errors, it's ok if we lose some oddball data
            wreport::options::LocalOverride<bool> o(wreport::options::var_silent_domain_errors, true);

            bulletin = BufrBulletin::decode(rmsg.data, rmsg.pathname.c_str(), rmsg.offset);
        } catch (wreport::error& e) {
            // We can still try to handle partially decoded files

            // Not if the error was not a parse error
            if (e.code() != WR_ERR_PARSE) return;

            // Not if we didn't decode just one subset
            if (bulletin->subsets.size() != 1) return;

            // Not if the subset is empty
            if (bulletin->subsets[0].empty()) return;
        } catch (std::exception& e) {
            return;
        }

        // If there is more than one subset, we are done
        if (bulletin->subsets.size() != 1)
            return;

        // Try to parse as a dba_msg
        try {
            // Ignore domain errors, it's ok if we lose some oddball data
            wreport::options::LocalOverride<bool> o(wreport::options::var_silent_domain_errors, true);

            msgs = importer.from_bulletin(*bulletin);
        } catch (std::exception& e) {
            // If we cannot import it as a Msgs, we are done
            return;
        }

        // We decoded a different number of subsets than declared by the BUFR.
        // How could this happen? Treat it as an obscure BUFR and go no further
        if (msgs.size() != 1)
            return;

        msg = &msgs[0];

        // Set the product from the msg type
        ValueBag newvals;
        newvals.set("t", Value::createString(msg_type_name(Msg::downcast(*msg).type)));
        product->addValues(newvals);

        // Set reference time from date and time if available
        refine_reftime(*msg);
    }
};
}


bool Bufr::do_scan(Metadata& md)
{
    md.clear();

    BinaryMessage rmsg = file->read();
    if (!rmsg) return false;

    // Set source
    if (false)
        md.set_source_inline("bufr", vector<uint8_t>(rmsg.data.begin(), rmsg.data.end()));
    else {
        unique_ptr<Source> source = Source::createBlob("bufr", basedir, relname, rmsg.offset, rmsg.data.size());
        md.set_source(move(source));
        md.set_cached_data(vector<uint8_t>(rmsg.data.begin(), rmsg.data.end()));
    }

    Harvest harvest(*importer);
    harvest.harvest_from_dballe(rmsg, md);

    md.set(move(harvest.reftime));
    md.set(move(harvest.origin));
    md.set(move(harvest.product));

    if (extras && harvest.msg)
    {
        // DB-All.e managed to make sense of the message: hand it down to Lua
        // to extract further metadata
        extras->scan(*harvest.msg, md);
    }

    // Check that the date is a valid date, unset if it is rubbish
    const reftime::Position* rt = md.get<types::reftime::Position>();
    if (rt)
    {
        if (rt->time.vals[0] <= 0)
            md.unset(types::TYPE_REFTIME);
        else
        {
            types::Time t = rt->time;
            wibble::grcal::date::normalise(t.vals);
            if (t != rt->time) md.unset(types::TYPE_REFTIME);
        }
    }

    // Convert validity times to emission times
    // If md has a timedef, substract it from the reftime
    const timerange::Timedef* timedef = md.get<types::timerange::Timedef>();
    if (timedef)
        if (const reftime::Position* p = md.get<types::reftime::Position>())
            md.set(timedef->validity_time_to_emission_time(*p));

    return true;
}

bool Bufr::next(Metadata& md)
{
    return do_scan(md);
}

static inline void inplace_tolower(std::string& buf)
{
	for (std::string::iterator i = buf.begin(); i != buf.end(); ++i)
		*i = tolower(*i);
}

int Bufr::update_sequence_number(const std::string& buf)
{
    auto bulletin = BufrBulletin::decode_header(buf);
    return bulletin->update_sequence_number;
}

}
}
