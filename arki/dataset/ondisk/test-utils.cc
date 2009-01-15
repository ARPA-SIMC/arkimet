#include <arki/dataset/ondisk/test-utils.h>
#include <fstream>

using namespace std;
using namespace arki;
using namespace arki::types;

namespace arki {

size_t countDeletedMetadata(const std::string& fname)
{
	size_t count = 0;
	vector<Metadata> mds = Metadata::readFile(fname);
	for (vector<Metadata>::const_iterator i = mds.begin(); i != mds.end(); ++i)
		if (i->deleted)
			++count;
	return count;
}

}
// vim:set ts=4 sw=4:
