#include "config.h"
#include <arki/metadata/tests.h>
#include <arki/formatter.h>
#include <arki/metadata.h>
#include <arki/utils/files.h>
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
using namespace arki::utils;
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
        string s(str1.str());
        auto reader = files::linereader_from_chars(s.data(), s.size());
        md1.readYaml(*reader, "(test memory buffer)");
    }
    Metadata md2;
    {
        string s(str2.str());
        auto reader = files::linereader_from_chars(s.data(), s.size());
        md2.readYaml(*reader, "(test memory buffer)");
    }

	// Once reparsed, they should have the same content
	ensure_equals(md, md1);
	ensure_equals(md, md2);
}

}
