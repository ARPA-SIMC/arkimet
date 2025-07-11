#ifndef ARKI_ITEMSET_H
#define ARKI_ITEMSET_H

#include <arki/types/fwd.h>
#include <memory>
#include <vector>

namespace arki {
namespace types {

class ItemSet
{
protected:
    std::vector<std::pair<types::Code, types::Type*>> m_vals;

    void sort();

public:
    typedef std::vector<std::pair<types::Code, types::Type*>>::const_iterator
        const_iterator;

    ItemSet();
    ItemSet(const ItemSet&);
    ~ItemSet();
    ItemSet& operator=(const ItemSet&);

    const_iterator begin() const { return m_vals.begin(); }
    const_iterator end() const { return m_vals.end(); }
    size_t empty() const { return m_vals.empty(); }
    size_t size() const { return m_vals.size(); }
    bool has(types::Code code) const;
    const types::Type* get(types::Code code) const;
    template <typename T> const T* get() const
    {
        const types::Type* i = get(types::traits<T>::type_code);
        if (!i)
            return 0;
        return dynamic_cast<const T*>(i);
    }

    /// Set an item
    void set(const types::Type& item);

    void set(std::unique_ptr<types::Type> item);

    template <typename T> void set(std::unique_ptr<T> i)
    {
        set(std::unique_ptr<types::Type>(i.release()));
    }

    /// Set an item, from strings. Useful for quickly setting up data in tests.
    void set(const std::string& type, const std::string& val);

    /// Unset an item
    void unset(types::Code code);

    /// Remove all items
    void clear();

    /**
     * Check that two ItemSets contain the same information
     */
    bool operator==(const ItemSet& m) const;

    /**
     * Check that two ItemSets contain different information
     */
    bool operator!=(const ItemSet& m) const { return !operator==(m); }

    int compare(const ItemSet& m) const;
    bool operator<(const ItemSet& o) const { return compare(o) < 0; }
    bool operator<=(const ItemSet& o) const { return compare(o) <= 0; }
    bool operator>(const ItemSet& o) const { return compare(o) > 0; }
    bool operator>=(const ItemSet& o) const { return compare(o) >= 0; }
};

} // namespace types
} // namespace arki

#endif
