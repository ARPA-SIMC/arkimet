#include "config.h"

#include <arki/dataset/empty.h>

using namespace std;

namespace arki {
namespace dataset {

Empty::Empty(const ConfigFile& cfg)
	: LocalReader(cfg)
{
}

Empty::~Empty()
{
}

}
}
