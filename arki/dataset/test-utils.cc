#include <arki/dataset/test-utils.h>
#include <arki/metadata.h>
#include <arki/dispatcher.h>
#include <fstream>

using namespace std;
using namespace arki;
using namespace arki::types;

namespace arki {
namespace tests {

struct MetadataCollector : public vector<Metadata>, public MetadataConsumer
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		return true;
	}
};

void impl_ensure_dispatches(const wibble::tests::Location& loc, Dispatcher& dispatcher, Metadata& md, MetadataConsumer& mdc)
{
	MetadataCollector c;
	Dispatcher::Outcome res = dispatcher.dispatch(md, c);
	// If dispatch fails, print the notes
	if (res != Dispatcher::DISP_OK)
	{
		for (vector<Metadata>::iterator i = c.begin(); i != c.end(); ++i)
		{
			cerr << "Failed dispatch notes:" << endl;
			for (std::vector< Item<types::Note> >::const_iterator j = i->notes.begin();
					j != i->notes.end(); ++j)
				cerr << "   " << *j << endl;
		}
	}
	inner_ensure_equals(res, Dispatcher::DISP_OK);
	for (vector<Metadata>::iterator i = c.begin(); i != c.end(); ++i)
		mdc(*i);
}

}
}
// vim:set ts=4 sw=4:
