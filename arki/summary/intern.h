#ifndef ARKI_SUMMARY_INTERN_H
#define ARKI_SUMMARY_INTERN_H

#include <arki/types/typeset.h>

namespace arki {
namespace summary {

/**
 * Maintain a set of unique pointers for Type items.
 *
 * It allows to reuse owner pointers and create structures where equality
 * between items can be tested just by comparing pointers.
 */
class TypeIntern
{
protected:
    types::TypeSet known_items;

public:
    TypeIntern();
    ~TypeIntern();

    typedef types::TypeSet::const_iterator const_iterator;

    const_iterator begin() const { return known_items.begin(); }
    const_iterator end() const { return known_items.end(); }

    /// Lookup an item, returning 0 if we have never seen it
    const types::Type* lookup(const types::Type& item) const;

    /**
     * Lookup an item, adding a clone of it to the collection if we have never
     * seen it
     */
    const types::Type* intern(const types::Type& item);

    /**
     * Same as intern(const Type&) but if the item is missing, it reuses this
     * one instead of cloning a new one
     */
    const types::Type* intern(std::unique_ptr<types::Type>&& item);

private:
    TypeIntern(const TypeIntern&);
    TypeIntern& operator=(const TypeIntern&);
};

}
}

#endif
