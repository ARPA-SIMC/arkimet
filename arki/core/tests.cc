#include "tests.h"
#include <arki/binary.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>

using namespace std;
using namespace arki;
using namespace arki::tests;

namespace arki {
namespace tests {

void ActualTime::operator==(const std::string& expected) const
{
    operator==(types::Time::createFromISO8601(expected));
}

void ActualTime::operator!=(const std::string& expected) const
{
    operator!=(types::Time::createFromISO8601(expected));
}

void ActualTime::serializes() const
{
    // Binary encoding, without envelope
    std::vector<uint8_t> enc;
    BinaryEncoder e(enc);
    _actual.encodeWithoutEnvelope(e);
    BinaryDecoder dec(enc);
    wassert(actual(types::Time::decode(dec)) == _actual);

    // String encoding
    stringstream ss;
    ss << _actual;
    wassert(actual(types::Time::createFromISO8601(ss.str())) == _actual);

    // JSON encoding
    {
        std::stringstream jbuf;
        emitter::JSON json(jbuf);
        _actual.serialise(json);
        jbuf.seekg(0);
        emitter::Memory parsed;
        emitter::JSON::parse(jbuf, parsed);
        wassert(actual(parsed.root().is_mapping()).istrue());
        types::Time iparsed = types::Time::decodeMapping(parsed.root().get_mapping());
        wassert(actual(iparsed) == _actual);
    }
}

void ActualTime::compares(const types::Time& higher) const
{
    types::Time higher2 = higher;

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
    wassert(actual(_actual.vals[0]) == ye);
    wassert(actual(_actual.vals[1]) == mo);
    wassert(actual(_actual.vals[2]) == da);
    wassert(actual(_actual.vals[3]) == ho);
    wassert(actual(_actual.vals[4]) == mi);
    wassert(actual(_actual.vals[5]) == se);
}

}
}
