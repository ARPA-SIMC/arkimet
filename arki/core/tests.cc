#include "tests.h"
#include <arki/binary.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>

using namespace std;
using namespace arki;
using namespace arki::tests;
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
    BinaryEncoder e(enc);
    _actual.encodeWithoutEnvelope(e);
    BinaryDecoder dec(enc);
    wassert(actual(Time::decode(dec)) == _actual);

    // String encoding
    stringstream ss;
    ss << _actual;
    wassert(actual(Time::create_iso8601(ss.str())) == _actual);

    // JSON encoding
    {
        std::stringstream jbuf;
        emitter::JSON json(jbuf);
        _actual.serialise(json);
        jbuf.seekg(0);
        emitter::Memory parsed;
        emitter::JSON::parse(jbuf, parsed);
        wassert(actual(parsed.root().is_mapping()).istrue());
        Time iparsed = Time::decodeMapping(parsed.root().get_mapping());
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
    wassert(actual(higher >  higher2).isfalse());
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

}
}
