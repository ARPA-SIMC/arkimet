#include "arki/exceptions.h"
#include "arki/core/time.h"
#include "arki/libconfig.h"
#include "memory.h"
#include <memory>

using namespace std;

namespace arki {
namespace emitter {

namespace memory {

Node::~Node()
{
}

void Node::add_val(const memory::Node*)
{
    throw_consistency_error("adding node to structured data", "cannot add elements to this node");
}

List::~List()
{
    for (auto& i: val)
        delete i;
}

void List::add_val(const memory::Node* n)
{
    val.push_back(n);
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

void Mapping::add_val(const memory::Node* n)
{
    if (has_cur_key)
    {
        val.insert(make_pair(cur_key, n));
        has_cur_key = false;
    }
    else
    {
        if (!n->is_string())
            throw_consistency_error("adding node to structured data", "cannot use a non-string as mapping key");
        cur_key = n->get_string();
        has_cur_key = true;
        delete n;
    }
}

bool Mapping::has_key(const std::string& key, NodeType type) const
{
    auto i = val.find(key);
    if (i == val.end())
        return false;

    if (i->second->type() != type)
        return false;

    return true;
}

core::Time Mapping::as_time(const std::string& key, const char* desc) const
{
    auto i = val.find(key);
    if (i == val.end())
        return Node::as_time(key, desc);

    const List* list = dynamic_cast<const List*>(i->second);
    if (!list)
        return Node::as_time(key, desc);

    if (list->size() < 6)
        throw std::runtime_error("cannot decode item: list has " + std::to_string(list->size()) + " elements instead of 6");

    core::Time res;
    res.ye = list->as_int(0, "time year");
    res.mo = list->as_int(1, "time month");
    res.da = list->as_int(2, "time day");
    res.ho = list->as_int(3, "time hour");
    res.mi = list->as_int(4, "time minute");
    res.se = list->as_int(5, "time second");
    return res;
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
