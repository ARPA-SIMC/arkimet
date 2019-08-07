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

struct List;
struct Mapping;

struct Node : public Reader
{
    virtual ~Node();
    virtual const char* tag() const = 0;
    virtual void add_val(const Node*);
};

struct Null : public Node
{
    const char* tag() const override { return "null"; }
    NodeType type() const override { return NodeType::NONE; }
    std::string repr() const override { return "null"; }
};

struct Bool : public Node
{
    bool val;
    Bool(bool val) { this->val = val; }
    const char* tag() const override { return "bool"; }
    NodeType type() const override { return NodeType::BOOL; }
    std::string repr() const override { return val ? "true" : "false"; }
    bool as_bool(const char* desc) const override { return val; }
};

struct Int : public Node
{
    long long int val;
    Int(long long int val) { this->val = val; }
    const char* tag() const override { return "int"; }
    NodeType type() const override { return NodeType::INT; }
    std::string repr() const override { return std::to_string(val); }
    long long int as_int(const char* desc) const override { return val; }
};

struct Double : public Node
{
    double val;
    Double(double val) { this->val = val; }
    const char* tag() const override { return "double"; }
    NodeType type() const override { return NodeType::DOUBLE; }
    std::string repr() const override { return std::to_string(val); }
    double as_double(const char* desc) const override { return val; }
};

struct String : public Node
{
    std::string val;
    String(const std::string& val) { this->val = val; }
    const char* tag() const override { return "string"; }
    NodeType type() const override { return NodeType::STRING; }
    std::string repr() const override;
    std::string as_string(const char* desc) const override { return val; }
};

struct List : public Node
{
    std::vector<const memory::Node*> val;

    ~List();

    const char* tag() const override { return "list"; }
    void add_val(const memory::Node* n) override;

    bool empty() const { return val.empty(); }
    size_t size() const { return val.size(); }
    const memory::Node& operator[](size_t idx) const { return *val[idx]; }

    NodeType type() const override { return NodeType::LIST; }
    std::string repr() const override;
    core::Time as_time(const char* desc) const override;
    unsigned list_size(const char* desc) const override { return size(); }
    bool as_bool(unsigned idx, const char* desc) const override { return val[idx]->as_bool(desc); }
    long long int as_int(unsigned idx, const char* desc) const override { return val[idx]->as_int(desc); }
    double as_double(unsigned idx, const char* desc) const override { return val[idx]->as_double(desc); }
    std::string as_string(unsigned idx, const char* desc) const override { return val[idx]->as_string(desc); }
    std::unique_ptr<types::Type> as_type(unsigned idx, const char* desc, const structured::Keys& keys) const;
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
    bool has_key(const std::string& key, NodeType type) const override;
    bool as_bool(const std::string& key, const char* desc) const override { return (*this)[key].as_bool(desc); }
    long long int as_int(const std::string& key, const char* desc) const override { return (*this)[key].as_int(desc); }
    double as_double(const std::string& key, const char* desc) const override { return (*this)[key].as_double(desc); }
    std::string as_string(const std::string& key, const char* desc) const override { return (*this)[key].as_string(desc); }
    core::Time as_time(const std::string& key, const char* desc) const override;
    std::unique_ptr<types::Type> as_type(const std::string& key, const char* desc, const structured::Keys& keys) const;
    void sub(const std::string& key, const char* desc, std::function<void(const Reader&)> dest) const override { dest((*this)[key]); }
    void items(const char* desc, std::function<void(const std::string&, const Reader&)> dest) const override
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
