#include "typevector.h"
#include "arki/types.h"
#include <algorithm>

using namespace std;
using namespace arki::types;

namespace arki {
namespace types {

TypeVector::TypeVector() {}

TypeVector::TypeVector(const TypeVector& o)
{
    vals.reserve(o.vals.size());
    for (const_iterator i = o.begin(); i != o.end(); ++i)
        vals.push_back(*i ? (*i)->clone() : 0);
}

TypeVector::~TypeVector()
{
    for (vector<Type*>::iterator i = vals.begin(); i != vals.end(); ++i)
        delete *i;
}

bool TypeVector::operator==(const TypeVector& o) const
{
    if (size() != o.size())
        return false;
    const_iterator a = begin();
    const_iterator b = o.begin();
    while (a != end() && b != o.end())
        if (!Type::nullable_equals(*a, *b))
            return false;
    return true;
}

void TypeVector::set(size_t pos, std::unique_ptr<types::Type> val)
{
    if (pos >= vals.size())
        vals.resize(pos + 1);
    else if (vals[pos])
        delete vals[pos];
    vals[pos] = val.release();
}

void TypeVector::set(size_t pos, const Type* val)
{
    if (pos >= vals.size())
        vals.resize(pos + 1);
    else if (vals[pos])
        delete vals[pos];

    if (val)
        vals[pos] = val->clone();
    else
        vals[pos] = 0;
}

void TypeVector::unset(size_t pos)
{
    if (pos >= vals.size())
        return;
    delete vals[pos];
    vals[pos] = 0;
}

void TypeVector::resize(size_t new_size)
{
    if (new_size < size())
    {
        // If we are shrinking, we need to deallocate the elements that are left
        // out
        for (size_t i = new_size; i < size(); ++i)
            delete vals[i];
    }

    // For everything else, just go with the vector default of padding with
    // zeroes
    vals.resize(new_size);
    return;
}

void TypeVector::rtrim()
{
    while (!vals.empty() && !vals.back())
        vals.pop_back();
}

void TypeVector::split(size_t pos, TypeVector& dest)
{
    dest.vals.reserve(dest.size() + size() - pos);
    for (unsigned i = pos; i < size(); ++i)
        dest.vals.push_back(vals[i]);
    vals.resize(pos);
}

void TypeVector::push_back(std::unique_ptr<types::Type>&& val)
{
    vals.push_back(val.release());
}

void TypeVector::push_back(const types::Type& val)
{
    vals.push_back(val.clone());
}

namespace {
struct TypeptrLt
{
    inline bool operator()(const types::Type* a, const types::Type* b) const
    {
        return *a < *b;
    }
};
} // namespace

TypeVector::const_iterator TypeVector::sorted_find(const Type& type) const
{
    const_iterator lb =
        lower_bound(vals.begin(), vals.end(), &type, TypeptrLt());
    if (lb == vals.end())
        return vals.end();
    if (**lb != type)
        return vals.end();
    return lb;
}

bool TypeVector::sorted_insert(const Type& item)
{
    vector<Type*>::iterator lb =
        lower_bound(vals.begin(), vals.end(), &item, TypeptrLt());
    if (lb == vals.end())
        push_back(item);
    else if (**lb != item)
        vals.insert(lb, item.clone());
    else
        return false;
    return true;
}

bool TypeVector::sorted_insert(std::unique_ptr<types::Type>&& item)
{
    vector<Type*>::iterator lb =
        lower_bound(vals.begin(), vals.end(), item.get(), TypeptrLt());
    if (lb == vals.end())
        push_back(move(item));
    else if (**lb != *item)
        vals.insert(lb, item.release());
    else
        return false;
    return true;
}

} // namespace types
} // namespace arki
