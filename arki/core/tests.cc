#include "tests.h"
#include "arki/core/binary.h"
#include "arki/core/file.h"
#include "arki/exceptions.h"
#include "arki/libconfig.h"
#include "arki/structured/json.h"
#include "arki/structured/memory.h"
#include "arki/utils/files.h"
#include <cstdlib>

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace tests {

void ActualTime::operator==(const std::string& expected) const
{
    operator==(Time::create_iso8601(expected));
}

void ActualTime::operator!=(const std::string& expected) const
{
    operator!=(Time::create_iso8601(expected));
}

void ActualTime::serializes() const
{
    // Binary encoding, without envelope
    std::vector<uint8_t> enc;
    core::BinaryEncoder e(enc);
    _actual.encodeWithoutEnvelope(e);
    core::BinaryDecoder dec(enc);
    wassert(actual(Time::decode(dec)) == _actual);

    // String encoding
    stringstream ss;
    ss << _actual;
    wassert(actual(Time::create_iso8601(ss.str())) == _actual);

    // JSON encoding
    {
        std::stringstream jbuf;
        structured::JSON json(jbuf);
        json.add(_actual);
        structured::Memory parsed;
        std::string str = jbuf.str();
        auto reader     = core::BufferedReader::from_string(str);
        structured::JSON::parse(*reader, parsed);
        wassert(actual(parsed.root().type()) == structured::NodeType::LIST);
        Time iparsed = parsed.root().as_time("time");
        wassert(actual(iparsed) == _actual);
    }
}

void ActualTime::compares(const Time& higher) const
{
    Time higher2 = higher;

    wassert(actual(_actual == _actual).istrue());
    wassert(actual(higher == higher).istrue());
    wassert(actual(higher2 == higher2).istrue());

    wassert(actual(_actual < higher).istrue());
    wassert(actual(_actual <= higher).istrue());
    wassert(actual(_actual == higher).isfalse());
    wassert(actual(_actual != higher).istrue());
    wassert(actual(_actual >= higher).isfalse());
    wassert(actual(_actual > higher).isfalse());

    wassert(actual(higher < higher2).isfalse());
    wassert(actual(higher <= higher2).istrue());
    wassert(actual(higher == higher2).istrue());
    wassert(actual(higher != higher2).isfalse());
    wassert(actual(higher >= higher2).istrue());
    wassert(actual(higher > higher2).isfalse());
}

void ActualTime::is(int ye, int mo, int da, int ho, int mi, int se)
{
    wassert(actual(_actual.ye) == ye);
    wassert(actual(_actual.mo) == mo);
    wassert(actual(_actual.da) == da);
    wassert(actual(_actual.ho) == ho);
    wassert(actual(_actual.mi) == mi);
    wassert(actual(_actual.se) == se);
}

void skip_unless_libzip()
{
#ifndef HAVE_LIBZIP
    throw TestSkipped("libzip not available");
#endif
}
void skip_unless_libarchive()
{
#ifndef HAVE_LIBARCHIVE
    throw TestSkipped("libarchive not available");
#endif
}
void skip_unless_grib()
{
#ifndef HAVE_GRIBAPI
    throw TestSkipped("GRIB support not available");
#endif
}
void skip_unless_bufr()
{
#ifndef HAVE_DBALLE
    throw TestSkipped("BUFR support not available");
#endif
}
void skip_unless_vm2()
{
#ifndef HAVE_VM2
    throw TestSkipped("VM2 support not available");
#endif
}
void skip_unless_geos()
{
#ifndef HAVE_GEOS
    throw TestSkipped("GEOS support not available");
#endif
}
void skip_unless_splice()
{
#ifndef ARKI_HAVE_SPLICE
    throw TestSkipped("splice() syscall is not available");
#endif
}

void skip_unless_filesystem_has_holes(const std::string& path)
{
    if (!utils::files::filesystem_has_holes(path))
        throw TestSkipped("Filesystem does not support holes in files");
}

void skip_unless_filesystem_has_ofd_locks(const std::string& path)
{
    if (!utils::files::filesystem_has_ofd_locks(path))
        throw TestSkipped("OFD locks not supported");
}

void delete_if_exists(const std::string& name)
{
    if (std::filesystem::is_directory(name))
        sys::rmtree(name);
    else if (std::filesystem::exists(name))
        sys::unlink(name);
}

} // namespace tests
} // namespace arki
