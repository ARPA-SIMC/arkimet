#include "itemset.h"
#include "arki/types.h"

using namespace std;
using namespace arki::types;

namespace arki {
namespace types {

ItemSet::ItemSet() {}

ItemSet::ItemSet(const ItemSet& o)
{
    m_vals.reserve(o.size());
    for (const auto& i: o)
        m_vals.emplace_back(i.first, i.second->clone());
}

ItemSet::~ItemSet()
{
    for (auto& i: m_vals)
        delete i.second;
}

ItemSet& ItemSet::operator=(const ItemSet& o)
{
    if (this == &o) return *this;
    clear();
    m_vals.reserve(o.size());
    for (const auto& i: o)
        m_vals.emplace_back(i.first, i.second->clone());
    return *this;
}

bool ItemSet::has(types::Code code) const
{
    for (const auto& i: m_vals)
        if (i.first == code)
            return true;
    return false;
}

const Type* ItemSet::get(Code code) const
{
    for (const auto& i: m_vals)
        if (i.first == code)
            return i.second;
    return nullptr;
}

void ItemSet::set(const Type& item)
{
    std::unique_ptr<Type> clone(item.clone());
    set(std::move(clone));
}

void ItemSet::set(std::unique_ptr<Type> item)
{
    // TODO: in theory, this could be rewritten with rbegin/rend to optimize
    // for the insertion of sorted data. In practice, after trying, it caused a
    // decrease in performance, so abandoning that for now
    Code code = item->type_code();
    for (auto i = m_vals.begin(); i != m_vals.end(); ++i)
    {
        if (i->first == code)
        {
            delete i->second;
            i->second = item.release();
            return;
        } else if (i->first > code) {
            m_vals.emplace(i, code, item.release());
            return;
        }
    }

    m_vals.emplace_back(code, item.release());
}

void ItemSet::set(const std::string& type, const std::string& val)
{
    set(decodeString(parseCodeName(type.c_str()), val));
}

void ItemSet::unset(Code code)
{
    for (auto i = m_vals.begin(); i != m_vals.end(); ++i)
        if (i->first == code)
        {
            delete i->second;
            m_vals.erase(i);
            return;
        }
}

void ItemSet::clear()
{
    for (auto& i: m_vals)
        delete i.second;
    m_vals.clear();
}

bool ItemSet::operator==(const ItemSet& m) const
{
    auto it1 = m_vals.begin();
    auto it2 = m.m_vals.begin();
    while (it1 != m_vals.end() && it2 != m.m_vals.end())
    {
        if (it1->first != it2->first) return false;
        if (!it1->second->equals(*it2->second)) return false;
        ++it1;
        ++it2;
    }
    return it1 == m_vals.end() && it2 == m.m_vals.end();
}

int ItemSet::compare(const ItemSet& m) const
{
    auto a = begin();
    auto b = m.begin();
    for ( ; a != end() && b != m.end(); ++a, ++b)
    {
        if (a->first < b->first)
            return -1;
        if (a->first > b->first)
            return 1;
        if (int res = a->second->compare(*b->second)) return res;
    }
    if (a == end() && b == m.end())
        return 0;
    if (a == end())
        return -1;
    return 1;
}

}
}
