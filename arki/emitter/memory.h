#ifndef ARKI_EMITTER_MEMORY_H
#define ARKI_EMITTER_MEMORY_H

/*
 * emitter/memory - Emitter storing structured data in memory
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/emitter.h>
#include <arki/wibble/exception.h>
#include <vector>
#include <map>
#include <string>

namespace arki {
namespace emitter {

namespace memory {

struct List;
struct Mapping;

struct Node
{
    virtual ~Node();

    virtual bool is_null() const { return false; }
    virtual bool is_bool() const { return false; }
    virtual bool is_int() const { return false; }
    virtual bool is_double() const { return false; }
    virtual bool is_string() const { return false; }
    virtual bool is_list() const { return false; }
    virtual bool is_mapping() const { return false; }

    void want_null(const char* context) const
    {
        if (!is_null()) throw wibble::exception::Consistency(context, "item is not null");
    }
    const bool& want_bool(const char* context) const
    {
        if (!is_bool()) throw wibble::exception::Consistency(context, "item is not bool");
        return get_bool();
    }
    const long long int& want_int(const char* context) const
    {
        if (!is_int()) throw wibble::exception::Consistency(context, "item is not int");
        return get_int();
    }
    const double& want_double(const char* context) const
    {
        if (!is_double()) throw wibble::exception::Consistency(context, "item is not double");
        return get_double();
    }
    const std::string& want_string(const char* context) const
    {
        if (!is_string()) throw wibble::exception::Consistency(context, "item is not string");
        return get_string();
    }
    const List& want_list(const char* context) const
    {
        if (!is_list()) throw wibble::exception::Consistency(context, "item is not list");
        return get_list();
    }
    const Mapping& want_mapping(const char* context) const
    {
        if (!is_mapping()) throw wibble::exception::Consistency(context, "item is not mapping");
        return get_mapping();
    }

    virtual const char* tag() const = 0;

    virtual void add_val(const Node*);

    const bool&          get_bool() const;
    const long long int& get_int() const;
    const double&        get_double() const;
    const std::string&   get_string() const;
    const List&          get_list() const;
    const Mapping&       get_mapping() const;
};

struct Null : public Node
{
    virtual bool is_null() const { return true; }
    virtual const char* tag() const { return "null"; }
};

struct Bool : public Node
{
    bool val;
    Bool(bool val) { this->val = val; }
    virtual bool is_bool() const { return true; }
    virtual const char* tag() const { return "bool"; }
};

struct Int : public Node
{
    long long int val;
    Int(long long int val) { this->val = val; }
    virtual bool is_int() const { return true; }
    virtual const char* tag() const { return "int"; }
};

struct Double : public Node
{
    double val;
    Double(double val) { this->val = val; }
    virtual bool is_double() const { return true; }
    virtual const char* tag() const { return "double"; }
};

struct String : public Node
{
    std::string val;
    String(const std::string& val) { this->val = val; }
    virtual bool is_string() const { return true; }
    virtual const char* tag() const { return "string"; }
};

struct List : public Node
{
    std::vector<const Node*> val;

    ~List();

    virtual bool is_list() const { return true; }
    virtual const char* tag() const { return "list"; }
    virtual void add_val(const Node* n);

    bool empty() const { return val.empty(); }
    size_t size() const { return val.size(); }
    const Node& operator[](size_t idx) const { return *val[idx]; }
};

struct Mapping : public Node
{
    std::map<std::string, const Node*> val;
    Null default_val;
    bool has_cur_key;
    std::string cur_key;

    Mapping();
    ~Mapping();

    virtual bool is_mapping() const { return true; }
    virtual const char* tag() const { return "mapping"; }
    virtual void add_val(const Node* n);

    bool empty() const { return val.empty(); }
    size_t size() const { return val.size(); }
    const Node& operator[](const std::string& idx) const
    {
        std::map<std::string, const Node*>::const_iterator i = val.find(idx);
        if (i == val.end()) return default_val;
        return *i->second;
    }
};

inline const bool& Node::get_bool() const { return dynamic_cast<const Bool*>(this)->val; }
inline const long long int& Node::get_int() const { return dynamic_cast<const Int*>(this)->val; }
inline const double& Node::get_double() const { return dynamic_cast<const Double*>(this)->val; }
inline const std::string& Node::get_string() const { return dynamic_cast<const String*>(this)->val; }
inline const List& Node::get_list() const { return *dynamic_cast<const List*>(this); }
inline const Mapping& Node::get_mapping() const { return *dynamic_cast<const Mapping*>(this); }

}

/**
 * Emitter storing structured data in memory
 */
class Memory : public Emitter
{
protected:
    memory::Node* m_root;
    std::vector<memory::Node*> stack;

    void add_val(memory::Node* val);

public:
    Memory();
    virtual ~Memory();

    virtual void start_list();
    virtual void end_list();

    virtual void start_mapping();
    virtual void end_mapping();

    virtual void add_null();
    virtual void add_bool(bool val);
    virtual void add_int(long long int val);
    virtual void add_double(double val);
    virtual void add_string(const std::string& val);

    const memory::Node& root() const { return *m_root; }
};

}
}

// vim:set ts=4 sw=4:
#endif
