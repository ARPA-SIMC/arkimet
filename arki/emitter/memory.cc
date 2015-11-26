#include "config.h"

#include <arki/emitter/memory.h>
#include <memory>

using namespace std;

namespace arki {
namespace emitter {

namespace memory {

Node::~Node()
{
}

void Node::add_val(const Node*)
{
    throw wibble::exception::Consistency("adding node to structured data", "cannot add elements to this node");
}

List::~List()
{
    for (vector<const Node*>::iterator i = val.begin();
            i != val.end(); ++i)
        delete *i;
}

void List::add_val(const Node* n)
{
    val.push_back(n);
}

Mapping::Mapping()
    : has_cur_key(false)
{
}

Mapping::~Mapping()
{
    for (map<string, const Node*>::iterator i = val.begin();
            i != val.end(); ++i)
        delete i->second;
}

void Mapping::add_val(const Node* n)
{
    if (has_cur_key)
    {
        val.insert(make_pair(cur_key, n));
        has_cur_key = false;
    }
    else
    {
        if (!n->is_string())
            throw wibble::exception::Consistency("adding node to structured data", "cannot use a non-string as mapping key");
        cur_key = n->get_string();
        has_cur_key = true;
        delete n;
    }
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

// vim:set ts=4 sw=4:
