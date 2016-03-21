#ifndef ARKI_UTILS_COMMANDLINE_CORE_H
#define ARKI_UTILS_COMMANDLINE_CORE_H

#include <stdexcept>
#include <string>
#include <list>
#include <set>

namespace arki {
namespace utils {
namespace commandline {

struct BadOption : public std::runtime_error
{
    using std::runtime_error::runtime_error;
};

class ArgList : public std::list<std::string>
{
public:
	// Remove the item pointed by the iterator, and advance the iterator to the
	// next item.  Returns i itself.
	inline iterator& eraseAndAdvance(iterator& i)
	{
		if (i == end())
			return i;
		iterator next = i;
		++next;
		erase(i);
		i = next;
		return i;
	}

	static bool isSwitch(const char* str);
	static bool isSwitch(const std::string& str);
	static bool isSwitch(const const_iterator& iter);
	static bool isSwitch(const iterator& iter);
};

class Managed
{
public:
	virtual ~Managed() {}
};

/** Keep track of various wibble::commandline components, and deallocate them
 * at object destruction.
 *
 * If an object is added multiple times, it will still be deallocated only once.
 */
class MemoryManager
{
	std::set<Managed*> components;

	Managed* addManaged(Managed* o) { components.insert(o); return o; }
public:
	~MemoryManager()
	{
		for (std::set<Managed*>::const_iterator i = components.begin();
				i != components.end(); ++i)
			delete *i;
	}

	template<typename T>
	T* add(T* item) { addManaged(item); return item; }
};

}
}
}
#endif
