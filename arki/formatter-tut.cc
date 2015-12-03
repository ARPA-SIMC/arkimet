#include "config.h"

#include <arki/metadata/tests.h>
#include <arki/formatter.h>
#include <arki/metadata.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
    m.writeYaml(o);
	return o;
}
}

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_formatter_shar {
    Metadata md;

    arki_formatter_shar()
    {
        arki::tests::fill(md);
    }
};
TESTGRP(arki_formatter);

// See if the formatter makes a difference
template<> template<>
void to::test<1>()
{
	unique_ptr<Formatter> formatter(Formatter::create());

	stringstream str1;
	md.writeYaml(str1);

	stringstream str2;
	md.writeYaml(str2, formatter.get());
	
	// They must be different
	ensure(str1.str() != str2.str());

	// str2 contains annotations, so it should be longer
	ensure(str1.str().size() < str2.str().size());

    // Read back the two metadatas
    Metadata md1;
    {
        stringstream str(str1.str(), ios_base::in);
        md1.readYaml(str, "(test memory buffer)");
    }
    Metadata md2;
    {
        stringstream str(str2.str(), ios_base::in);
        md2.readYaml(str, "(test memory buffer)");
    }

	// Once reparsed, they should have the same content
	ensure_equals(md, md1);
	ensure_equals(md, md2);
}

}

// vim:set ts=4 sw=4:
