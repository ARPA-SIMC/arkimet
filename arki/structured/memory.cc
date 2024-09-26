#include "arki/exceptions.h"
#include "arki/core/time.h"
#include "arki/types.h"
#include "arki/utils/string.h"
#include "memory.h"
#include <memory>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace structured {

namespace memory {

Node::~Node()
{
}

void Node::add_val(const memory::Node*)
{
    throw_consistency_error("adding node to structured data", "cannot add elements to this node");
}

std::string String::repr() const { return "\"" + str::encode_cstring(val) + "\""; }

List::~List()
{
    for (auto& i: val)
        delete i;
}

void List::add_val(const memory::Node* n)
{
    val.push_back(n);
}

std::string List::repr() const
{
    std::string res("[");
    bool first = true;
    for (const auto& value: val)
    {
        if (first)
            first = false;
        else
            res += ", ";
        res += value->repr();
    }
    res += "]";
    return res;
}

core::Time List::scalar_as_time(const char* desc) const
{
    if (size() < 6)
        throw std::invalid_argument("cannot decode time: list has " + std::to_string(size()) + " elements instead of 6");

    core::Time res;
    res.ye = as_int(0, "time year");
    res.mo = as_int(1, "time month");
    res.da = as_int(2, "time day");
    res.ho = as_int(3, "time hour");
    res.mi = as_int(4, "time minute");
    res.se = as_int(5, "time second");
    return res;
}

std::unique_ptr<types::Type> List::list_as_type(unsigned idx, const char* desc, const structured::Keys& keys) const
{
    return types::decode_structure(keys, *val[idx]);
}

Mapping::Mapping()
    : has_cur_key(false)
{
}

Mapping::~Mapping()
{
    for (auto& i: val)
        delete i.second;
}

std::string Mapping::repr() const
{
    std::string res("{");
    bool first = true;
    for (const auto& value: val)
    {
        if (first)
            first = false;
        else
            res += ", ";
        res += "\"" + str::encode_cstring(value.first) + "\": " + value.second->repr();
    }
    res += "}";
    return res;
}

void Mapping::add_val(const memory::Node* n)
{
    if (has_cur_key)
    {
        val.insert(make_pair(cur_key, n));
        has_cur_key = false;
    }
    else
    {
        if (n->type() != NodeType::STRING)
            throw_consistency_error("adding node to structured data", "cannot use a non-string as mapping key");
        cur_key = n->as_string("key");
        has_cur_key = true;
        delete n;
    }
}

bool Mapping::dict_has_key(const std::string& key, NodeType type) const
{
    auto i = val.find(key);
    if (i == val.end())
        return false;

    if (i->second->type() != type)
        return false;

    return true;
}

core::Time Mapping::dict_as_time(const std::string& key, const char* desc) const
{
    auto i = val.find(key);
    if (i == val.end())
        return Node::as_time(key, desc);

    const List* list = dynamic_cast<const List*>(i->second);
    if (!list)
        return Node::as_time(key, desc);

    return list->as_time(desc);
}

std::unique_ptr<types::Type> Mapping::dict_as_type(const std::string& key, const char* desc, const structured::Keys& keys) const
{
    auto i = val.find(key);
    if (i == val.end())
        throw std::invalid_argument("cannot decode time: key " + key + " does not exist");
    return types::decode_structure(keys, *i->second);
}


}

Memory::Memory()
    : m_root(0)
{
}

Memory::~Memory()
{;
    if (m_root) delete m_root;
}

void Memory::add_val(memory::Node* val)
{
    if (!m_root)
        m_root = val;
    else
        stack.back()->add_val(val);
}

void Memory::add_null()
{
    add_val(new memory::Null);
}

void Memory::add_bool(bool val)
{
    add_val(new memory::Bool(val));
}

void Memory::add_int(long long int val)
{
    add_val(new memory::Int(val));
}

void Memory::add_double(double val)
{
    add_val(new memory::Double(val));
}

void Memory::add_string(const std::string& val)
{
    add_val(new memory::String(val));
}

void Memory::start_list()
{
    memory::List* val = new memory::List;
    add_val(val);
    stack.push_back(val);
}

void Memory::end_list()
{
    stack.pop_back();
}

void Memory::start_mapping()
{
    memory::Mapping* val = new memory::Mapping;
    add_val(val);
    stack.push_back(val);
}

void Memory::end_mapping()
{
    stack.pop_back();
}

}
}
