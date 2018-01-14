#include "itemset.h"
#include "types.h"
#include "utils.h"

using namespace std;
using namespace arki::types;

namespace arki {

ItemSet::ItemSet() {}

ItemSet::ItemSet(const ItemSet& o)
{
    for (const_iterator i = o.begin(); i != o.end(); ++i)
        m_vals.insert(make_pair(i->first, i->second->clone()));
}

ItemSet::~ItemSet()
{
    for (map<Code, Type*>::iterator i = m_vals.begin(); i != m_vals.end(); ++i)
        delete i->second;
}

ItemSet& ItemSet::operator=(const ItemSet& o)
{
    if (this == &o) return *this;
    clear();
    for (const_iterator i = o.begin(); i != o.end(); ++i)
        m_vals.insert(make_pair(i->first, i->second->clone()));
    return *this;
}

const Type* ItemSet::get(Code code) const
{
    const_iterator i = m_vals.find(code);
    if (i == m_vals.end()) return 0;
    return i->second;
}

void ItemSet::set(const Type& i)
{
    Code code = i.type_code();
    map<Code, Type*>::iterator it = m_vals.find(code);
    if (it == end())
        m_vals.insert(make_pair(code, i.clone()));
    else
    {
        delete it->second;
        it->second = i.clone();
    }
}

void ItemSet::set(unique_ptr<Type> i)
{
    Code code = i->type_code();
    map<Code, Type*>::iterator it = m_vals.find(code);
    if (it == m_vals.end())
        m_vals.insert(make_pair(code, i.release()));
    else
    {
        delete it->second;
        it->second = i.release();
    }
}

void ItemSet::set(const std::string& type, const std::string& val)
{
    set(decodeString(parseCodeName(type.c_str()), val));
}

void ItemSet::unset(Code code)
{
    map<Code, Type*>::iterator it = m_vals.find(code);
    if (it == m_vals.end()) return;
    delete it->second;
    m_vals.erase(it);
}

void ItemSet::clear()
{
    for (map<Code, Type*>::iterator i = m_vals.begin(); i != m_vals.end(); ++i)
        delete i->second;
    m_vals.clear();
}

bool ItemSet::operator==(const ItemSet& m) const
{
    map<Code, Type*>::const_iterator it1 = m_vals.begin();
    map<Code, Type*>::const_iterator it2 = m.m_vals.begin();
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
    const_iterator a = begin();
    const_iterator b = m.begin();
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
