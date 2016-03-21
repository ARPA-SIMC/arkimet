#include <arki/utils/commandline/core.h>
#include <cctype>
#include <cstring>

namespace arki {
namespace utils {
namespace commandline {

bool ArgList::isSwitch(const const_iterator& iter)
{
	return ArgList::isSwitch(*iter);
}

bool ArgList::isSwitch(const iterator& iter)
{
	return ArgList::isSwitch(*iter);
}


bool ArgList::isSwitch(const std::string& str)
{
	// No empty strings
	if (str[0] == 0)
		return false;
	// Must start with a dash
	if (str[0] != '-')
		return false;
	// Must not be "-" (usually it means 'stdin' file argument)
	if (str[1] == 0)
		return false;
	// Must not be "--" (end of switches)
	if (str == "--")
		return false;
	return true;
}

bool ArgList::isSwitch(const char* str)
{
	// No empty strings
	if (str[0] == 0)
		return false;
	// Must start with a dash
	if (str[0] != '-')
		return false;
	// Must not be "-" (usually it means 'stdin' file argument)
	if (str[1] == 0)
		return false;
	// Must not be "--" (end of switches)
	if (strcmp(str, "--") == 0)
		return false;
	return true;
}

}
}
}
