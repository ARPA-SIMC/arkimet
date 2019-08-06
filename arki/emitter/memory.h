#ifndef ARKI_EMITTER_MEMORY_H
#define ARKI_EMITTER_MEMORY_H

/// Emitter storing structured data in memory

#include <arki/emitter.h>
#include <arki/emitter/structure.h>
#include <vector>
#include <map>
#include <string>

namespace arki {
namespace emitter {

namespace memory {

struct List;
struct Mapping;

struct Node : public emitter::Reader
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
        if (!is_null()) throw std::runtime_error(std::string("item is not null when ") + context);
    }
    const bool& want_bool(const char* context) const
    {
        if (!is_bool()) throw std::runtime_error(std::string("item is not bool when ") + context);
        return get_bool();
    }
    const long long int& want_int(const char* context) const
    {
        if (!is_int()) throw std::runtime_error(std::string("item is not int when ") + context);
        return get_int();
    }
    const double& want_double(const char* context) const
    {
        if (!is_double()) throw std::runtime_error(std::string("item is not double when ") + context);
        return get_double();
    }
    const std::string& want_string(const char* context) const
    {
        if (!is_string()) throw std::runtime_error(std::string("item is not string when ") + context);
        return get_string();
    }
    const List& want_list(const char* context) const
    {
        if (!is_list()) throw std::runtime_error(std::string("item is not list when ") + context);
        return get_list();
    }
    const Mapping& want_mapping(const char* context) const
    {
        if (!is_mapping()) throw std::runtime_error(std::string("item is not mapping when ") + context);
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
    bool is_null() const override { return true; }
    const char* tag() const override { return "null"; }
    NodeType type() const override { return NodeType::NONE; }
};

struct Bool : public Node
{
    bool val;
    Bool(bool val) { this->val = val; }
    bool is_bool() const override { return true; }
    const char* tag() const override { return "bool"; }
    NodeType type() const override { return NodeType::BOOL; }
    bool as_bool(const char* desc) const override { return val; }
};

struct Int : public Node
{
    long long int val;
    Int(long long int val) { this->val = val; }
    bool is_int() const override { return true; }
    const char* tag() const override { return "int"; }
    NodeType type() const override { return NodeType::INT; }
    long long int as_int(const char* desc) const override { return val; }
};

struct Double : public Node
{
    double val;
    Double(double val) { this->val = val; }
    bool is_double() const override { return true; }
    const char* tag() const override { return "double"; }
    NodeType type() const override { return NodeType::DOUBLE; }
    double as_double(const char* desc) const override { return val; }
};

struct String : public Node
{
    std::string val;
    String(const std::string& val) { this->val = val; }
    bool is_string() const override { return true; }
    const char* tag() const override { return "string"; }
    NodeType type() const override { return NodeType::STRING; }
    std::string as_string(const char* desc) const override { return val; }
};

struct List : public Node
{
    std::vector<const memory::Node*> val;

    ~List();

    bool is_list() const override { return true; }
    const char* tag() const override { return "list"; }
    void add_val(const memory::Node* n) override;

    bool empty() const { return val.empty(); }
    size_t size() const { return val.size(); }
    const memory::Node& operator[](size_t idx) const { return *val[idx]; }

    NodeType type() const override { return NodeType::LIST; }
    unsigned list_size(const char* desc) const override { return size(); }
    bool as_bool(unsigned idx, const char* desc) const override { return val[idx]->want_bool(desc); }
    long long int as_int(unsigned idx, const char* desc) const override { return val[idx]->want_int(desc); }
    double as_double(unsigned idx, const char* desc) const override { return val[idx]->want_double(desc); }
    std::string as_string(unsigned idx, const char* desc) const override { return val[idx]->want_string(desc); }
    void sub(unsigned idx, const char* desc, std::function<void(const Reader&)> func) const override { func(*val[idx]); }
};

struct Mapping : public Node
{
    std::map<std::string, const memory::Node*> val;
    Null default_val;
    bool has_cur_key;
    std::string cur_key;

    Mapping();
    ~Mapping();

    bool is_mapping() const override { return true; }
    const char* tag() const override { return "mapping"; }
    void add_val(const memory::Node* n) override;

    bool empty() const { return val.empty(); }
    size_t size() const { return val.size(); }
    const memory::Node& operator[](const std::string& idx) const
    {
        std::map<std::string, const memory::Node*>::const_iterator i = val.find(idx);
        if (i == val.end()) return default_val;
        return *i->second;
    }

    NodeType type() const override { return NodeType::MAPPING; }
    bool has_key(const std::string& key, NodeType type) const override;
    bool as_bool(const std::string& key, const char* desc) const override { return (*this)[key].want_bool(desc); }
    long long int as_int(const std::string& key, const char* desc) const override { return (*this)[key].want_int(desc); }
    double as_double(const std::string& key, const char* desc) const override { return (*this)[key].want_double(desc); }
    std::string as_string(const std::string& key, const char* desc) const override { return (*this)[key].want_string(desc); }
    core::Time as_time(const std::string& key, const char* desc) const override;
    void sub(const std::string& key, const char* desc, std::function<void(const Reader&)> dest) const override { dest((*this)[key]); }
    void items(const char* desc, std::function<void(const std::string&, const Reader&)> dest) const override
    {
        for (const auto& i: val)
            dest(i.first, *i.second);
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
