#include "config.h"
#include "bufr.h"
#include "arki/utils/files.h"
#include <dballe/file.h>
#include <dballe/message.h>
#include <dballe/msg/codec.h>
#include <dballe/core/csv.h>
#include <wreport/bulletin.h>
#include <wreport/options.h>
#include "arki/metadata.h"
#include "arki/segment.h"
#include "arki/types/source.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/reftime.h"
#include "arki/types/run.h"
#include "arki/types/timerange.h"
#include "arki/scan/validator.h"
#include "arki/utils/sys.h"
#include <sstream>
#include <cstring>
#include <stdint.h>
#include <arpa/inet.h>

#ifdef HAVE_LUA
#include "arki/scan/bufrlua.h"
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
    std::string format() const override { return "BUFR"; }

    // Validate data found in a file
    void validate_file(sys::NamedFileDescriptor& fd, off_t offset, size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset, "file segment to check is only " + std::to_string(size) + " bytes (minimum for a BUFR is 8)");

        char buf[4];
        ssize_t res;
        if ((res = fd.pread(buf, 4, offset)) != 4)
            throw_check_error(fd, offset, "read only " + std::to_string(res) + "/4 bytes of BUFR header");
        if (memcmp(buf, "BUFR", 4) != 0)
            throw_check_error(fd, offset, "data does not start with 'BUFR'");
        if ((res = fd.pread(buf, 4, offset + size - 4)) != 4)
            throw_check_error(fd, offset, "read only " + std::to_string(res) + "/4 bytes of BUFR trailer");
        if (memcmp(buf, "7777", 4) != 0)
            throw_check_error(fd, offset, "data does not end with '7777'");
    }

    // Validate a memory buffer
    void validate_buf(const void* buf, size_t size) const override
    {
        if (size < 8)
            throw_check_error("buffer is shorter than 8 bytes");
        if (memcmp(buf, "BUFR", 4) != 0)
            throw_check_error("buffer does not start with 'BUFR'");
        if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
            throw_check_error("buffer does not end with '7777'");
    }
};

static BufrValidator bufr_validator;

const Validator& validator() { return bufr_validator; }

}


Bufr::Bufr() : file(0), importer(0), extras(0)
{
    msg::Importer::Options opts;
    opts.simplified = true;
    importer = msg::Importer::create(dballe::File::BUFR, opts).release();

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

void Bufr::open(const std::string& filename, std::shared_ptr<segment::Reader> reader)
{
    close();
    this->filename = filename;
    this->reader = reader;
    file = dballe::File::create(dballe::File::BUFR, filename.c_str(), "r").release();
}

void Bufr::close()
{
    filename.clear();
    reader.reset();
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
        reftime->time = core::Time(
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
        reftime = reftime::Position::create(core::Time(
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


void Bufr::do_scan(BinaryMessage& rmsg, Metadata& md)
{
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
        if (rt->time.ye <= 0)
            md.unset(TYPE_REFTIME);
        else
        {
            core::Time t = rt->time;
            t.normalise();
            if (t != rt->time) md.unset(TYPE_REFTIME);
        }
    }

    // Convert validity times to emission times
    // If md has a timedef, substract it from the reftime
    const timerange::Timedef* timedef = md.get<types::timerange::Timedef>();
    if (timedef)
        if (const reftime::Position* p = md.get<types::reftime::Position>())
            md.set(timedef->validity_time_to_emission_time(*p));
}

bool Bufr::next(Metadata& md)
{
    md.clear();
    BinaryMessage rmsg = file->read();
    if (!rmsg) return false;
    // Set source
    if (reader)
    {
        md.set_source(Source::createBlob(reader, rmsg.offset, rmsg.data.size()));
        md.set_cached_data(vector<uint8_t>(rmsg.data.begin(), rmsg.data.end()));
    } else
        md.set_source_inline("bufr", vector<uint8_t>(rmsg.data.begin(), rmsg.data.end()));
    do_scan(rmsg, md);
    return true;
}

std::unique_ptr<Metadata> Bufr::scan_data(const std::vector<uint8_t>& data)
{
    std::unique_ptr<Metadata> md(new Metadata);
    md->set_source_inline("grib", std::vector<uint8_t>(data));
    BinaryMessage rmsg(File::BUFR);
    rmsg.data = std::string(data.begin(), data.end());
    do_scan(rmsg, *md);
    return md;
}

bool Bufr::scan_file(const std::string& abspath, std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    open(abspath, reader);
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!next(*md)) break;
        if (!dest(move(md))) return false;
    }
    return true;
}

bool Bufr::scan_file_inline(const std::string& abspath, metadata_dest_func dest)
{
    open(abspath, std::shared_ptr<segment::Reader>());
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!next(*md)) break;
        if (!dest(move(md))) return false;
    }
    return true;
}

bool Bufr::scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest)
{
    throw std::runtime_error("scan_pipe not yet implemented for BUFR");
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
