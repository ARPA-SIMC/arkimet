#include "bufr.h"
#include <dballe/file.h>
#include <dballe/message.h>
#include <dballe/importer.h>
#include <wreport/bulletin.h>
#include <wreport/options.h>
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment/data.h"
#include "arki/types/source.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/reftime.h"
#include "arki/types/timerange.h"
#include "arki/types/values.h"
#include "arki/scan/validator.h"
#include "arki/scan/mock.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include <sstream>
#include <cstring>

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


/*
 * BufrScanner
 */

namespace {
class Harvest
{
protected:
    std::vector<std::shared_ptr<dballe::Message>> msgs;

public:
    dballe::Importer& importer;
    std::unique_ptr<Reftime> reftime;
    std::unique_ptr<Origin> origin;
    std::unique_ptr<Product> product;
    std::shared_ptr<Message> msg;

    Harvest(dballe::Importer& importer) : importer(importer), msg(0) {}

    void refine_reftime(const Message& msg)
    {
        Datetime dt;
        try {
            dt = msg.get_datetime();
        } catch (wreport::error_consistency&) {
            return;
        }
        if (dt.is_missing()) return;
        reftime = Reftime::createPosition(core::Time(
                dt.year, dt.month, dt.day,
                dt.hour, dt.minute, dt.second));
    }

    void harvest_from_dballe(const BinaryMessage& rmsg, Metadata& md)
    {
        msg.reset();
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
        reftime = Reftime::createPosition(core::Time(
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
            // We already set the importer to ignore domain errors
            msgs = importer.from_bulletin(*bulletin);
        } catch (std::exception& e) {
            // If we cannot import it as a Msgs, we are done
            return;
        }

        // We decoded a different number of subsets than declared by the BUFR.
        // How could this happen? Treat it as an obscure BUFR and go no further
        if (msgs.size() != 1)
            return;

        msg = msgs[0];

        // Set the product from the msg type
        types::values::ValueBagBuilder vbuilder;
        vbuilder.add("t", format_message_type(msg->get_type()));
        types::ValueBag newvals = vbuilder.build();
        product = Product::createBUFR(bulletin->data_category, bulletin->data_subcategory, bulletin->data_subcategory_local, newvals);

        // Set reference time from date and time if available
        refine_reftime(*msg);
    }
};
}


BufrScanner::BufrScanner()
{
    auto opts = ImporterOptions::create();
    opts->domain_errors = ImporterOptions::DomainErrors::UNSET;
    opts->simplified = true;
    importer = Importer::create(dballe::Encoding::BUFR, *opts).release();
}

BufrScanner::~BufrScanner()
{
    delete importer;
}

void BufrScanner::do_scan(BinaryMessage& rmsg, std::shared_ptr<Metadata> md)
{
    Harvest harvest(*importer);
    harvest.harvest_from_dballe(rmsg, *md);

    if (harvest.reftime)
        md->set(move(harvest.reftime));
    if (harvest.origin)
        md->set(move(harvest.origin));
    if (harvest.product)
        md->set(move(harvest.product));

    if (harvest.msg)
    {
        // DB-All.e managed to make sense of the message: let the subclasser
        // try to extract further metadata
        scan_extra(rmsg, harvest.msg, md);
    }

    // Check that the date is a valid date, unset if it is rubbish
    const reftime::Position* rt = md->get<types::reftime::Position>();
    if (rt)
    {
        auto time = rt->get_Position();
        if (time.ye <= 0)
            md->unset(TYPE_REFTIME);
        else
        {
            core::Time t = time;
            t.normalise();
            if (t != time)
                md->unset(TYPE_REFTIME);
        }
    }

    // Convert validity times to emission times
    // If md has a timedef, substract it from the reftime
    const timerange::Timedef* timedef = md->get<types::timerange::Timedef>();
    if (timedef)
        if (const reftime::Position* p = md->get<types::reftime::Position>())
            md->set(timedef->validity_time_to_emission_time(*p));
}

std::shared_ptr<Metadata> BufrScanner::scan_data(const std::vector<uint8_t>& data)
{
    auto md = std::make_shared<Metadata>();
    md->set_source_inline("grib", metadata::DataManager::get().to_data("bufr", std::vector<uint8_t>(data)));
    BinaryMessage rmsg(Encoding::BUFR);
    rmsg.data = std::string(data.begin(), data.end());
    do_scan(rmsg, md);
    return md;
}

bool BufrScanner::scan_segment(std::shared_ptr<segment::data::Reader> reader, metadata_dest_func dest)
{
    auto file = dballe::File::create(dballe::Encoding::BUFR, reader->segment().abspath.c_str(), "r");
    while (true)
    {
        auto md = std::make_shared<Metadata>();
        BinaryMessage rmsg = file->read();
        if (!rmsg) break;
        md->set_source(Source::createBlob(reader, rmsg.offset, rmsg.data.size()));
        md->set_cached_data(metadata::DataManager::get().to_data("bufr", vector<uint8_t>(rmsg.data.begin(), rmsg.data.end())));
        do_scan(rmsg, md);
        if (!dest(md)) return false;
    }
    return true;
}

std::shared_ptr<Metadata> BufrScanner::scan_singleton(const std::filesystem::path& abspath)
{
    auto md = std::make_shared<Metadata>();
    auto file = dballe::File::create(dballe::Encoding::BUFR, abspath.c_str(), "r");
    BinaryMessage rmsg = file->read();
    if (!rmsg) throw std::runtime_error(abspath.native() + " contains no BUFR data");
    do_scan(rmsg, md);
    if (file->read())
        throw std::runtime_error(abspath.native() + " contains more than one BUFR");
    return md;
}

bool BufrScanner::scan_pipe(core::NamedFileDescriptor& infd, metadata_dest_func dest)
{
    files::RAIIFILE in(infd, "rb");
    auto file = dballe::File::create(dballe::Encoding::BUFR, in, false, infd.path());
    while (true)
    {
        auto md = std::make_shared<Metadata>();
        BinaryMessage rmsg = file->read();
        if (!rmsg) break;
        md->set_source_inline("bufr", metadata::DataManager::get().to_data("bufr", vector<uint8_t>(rmsg.data.begin(), rmsg.data.end())));
        do_scan(rmsg, md);
        if (!dest(move(md))) return false;
    }
    return true;
}


int BufrScanner::update_sequence_number(const std::string& buf)
{
    auto bulletin = BufrBulletin::decode_header(buf);
    return bulletin->update_sequence_number;
}


/*
 * MockBufrScanner
 */

MockBufrScanner::MockBufrScanner()
{
    engine = new MockEngine();
}

MockBufrScanner::~MockBufrScanner()
{
    delete engine;
}

void MockBufrScanner::scan_extra(dballe::BinaryMessage& rmsg, std::shared_ptr<dballe::Message> msg, std::shared_ptr<Metadata> md)
{
    auto new_md = engine->lookup(reinterpret_cast<const uint8_t*>(rmsg.data.data()), rmsg.data.size());
    md->merge(*new_md);
}

}
}
