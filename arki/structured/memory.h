#ifndef ARKI_STRUCTURED_MEMORY_H
#define ARKI_STRUCTURED_MEMORY_H

/// Emitter storing structured data in memory

#include <arki/structured/emitter.h>
#include <arki/structured/reader.h>
#include <vector>
#include <map>
#include <string>

namespace arki {
namespace structured {

namespace memory {

class Node : public Reader
{
public:
    virtual ~Node();
    virtual const char* tag() const = 0;
    virtual void add_val(const Node*);
};

class Null : public Node
{
public:
    const char* tag() const override { return "null"; }
    NodeType type() const override { return NodeType::NONE; }
    std::string repr() const override { return "null"; }
};

class Bool : public Node
{
public:
    bool val;
    Bool(bool val) { this->val = val; }
    const char* tag() const override { return "bool"; }
    NodeType type() const override { return NodeType::BOOL; }
    std::string repr() const override { return val ? "true" : "false"; }
    bool scalar_as_bool(const char* desc) const override { return val; }
};

class Int : public Node
{
public:
    long long int val;
    Int(long long int val) { this->val = val; }
    const char* tag() const override { return "int"; }
    NodeType type() const override { return NodeType::INT; }
    std::string repr() const override { return std::to_string(val); }
    long long int scalar_as_int(const char* desc) const override { return val; }
};

class Double : public Node
{
public:
    double val;
    Double(double val) { this->val = val; }
    const char* tag() const override { return "double"; }
    NodeType type() const override { return NodeType::DOUBLE; }
    std::string repr() const override { return std::to_string(val); }
    double scalar_as_double(const char* desc) const override { return val; }
};

class String : public Node
{
public:
    std::string val;
    String(const std::string& val) { this->val = val; }
    const char* tag() const override { return "string"; }
    NodeType type() const override { return NodeType::STRING; }
    std::string repr() const override;
    std::string scalar_as_string(const char* desc) const override { return val; }
};

class List : public Node
{
public:
    std::vector<const memory::Node*> val;

    ~List();

    const char* tag() const override { return "list"; }
    void add_val(const memory::Node* n) override;

    bool empty() const { return val.empty(); }
    size_t size() const { return val.size(); }
    const memory::Node& operator[](size_t idx) const { return *val[idx]; }

    NodeType type() const override { return NodeType::LIST; }
    std::string repr() const override;
    core::Time scalar_as_time(const char* desc) const override;
    unsigned list_size(const char* desc) const override { return size(); }
    bool list_as_bool(unsigned idx, const char* desc) const override { return val[idx]->as_bool(desc); }
    long long int list_as_int(unsigned idx, const char* desc) const override { return val[idx]->as_int(desc); }
    double list_as_double(unsigned idx, const char* desc) const override { return val[idx]->as_double(desc); }
    std::string list_as_string(unsigned idx, const char* desc) const override { return val[idx]->as_string(desc); }
    std::unique_ptr<types::Type> list_as_type(unsigned idx, const char* desc, const structured::Keys& keys) const override;
    void list_sub(unsigned idx, const char* desc, std::function<void(const Reader&)> func) const override { func(*val[idx]); }
};

class Mapping : public Node
{
public:
    std::map<std::string, const memory::Node*> val;
    Null default_val;
    bool has_cur_key;
    std::string cur_key;

    Mapping();
    ~Mapping();

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
    std::string repr() const override;
    bool dict_has_key(const std::string& key, NodeType type) const override;
    bool dict_as_bool(const std::string& key, const char* desc) const override { return (*this)[key].as_bool(desc); }
    long long int dict_as_int(const std::string& key, const char* desc) const override { return (*this)[key].as_int(desc); }
    double dict_as_double(const std::string& key, const char* desc) const override { return (*this)[key].as_double(desc); }
    std::string dict_as_string(const std::string& key, const char* desc) const override { return (*this)[key].as_string(desc); }
    core::Time dict_as_time(const std::string& key, const char* desc) const override;
    std::unique_ptr<types::Type> dict_as_type(const std::string& key, const char* desc, const structured::Keys& keys) const override;
    void dict_sub(const std::string& key, const char* desc, std::function<void(const Reader&)> dest) const override { dest((*this)[key]); }
    void dict_items(const char* desc, std::function<void(const std::string&, const Reader&)> dest) const override
    {
        for (const auto& i: val)
            dest(i.first, *i.second);
    }
};

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

    void start_list() override;
    void end_list() override;

    void start_mapping() override;
    void end_mapping() override;

    void add_null() override;
    void add_bool(bool val) override;
    void add_int(long long int val) override;
    void add_double(double val) override;
    void add_string(const std::string& val) override;

    const memory::Node& root() const { return *m_root; }
};

}
}

#endif
