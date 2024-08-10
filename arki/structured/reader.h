#ifndef ARKI_STRUCTURED_READER_H
#define ARKI_STRUCTURED_READER_H

#include <arki/core/time.h>
#include <arki/types.h>
#include <arki/structured/fwd.h>
#include <string>
#include <memory>
#include <functional>
#include <iosfwd>

namespace arki {
namespace structured {

enum class NodeType : int
{
    NONE = 1,
    BOOL = 2,
    INT = 3,
    DOUBLE = 4,
    STRING = 5,
    LIST = 6,
    MAPPING = 7,
};

std::ostream& operator<<(std::ostream& o, NodeType type);


class Reader
{
public:
    virtual ~Reader() {}

    virtual NodeType type() const = 0;
    virtual std::string repr() const = 0;

    virtual bool scalar_as_bool(const char* desc) const;
    virtual long long int scalar_as_int(const char* desc) const;
    virtual double scalar_as_double(const char* desc) const;
    virtual std::string scalar_as_string(const char* desc) const;
    virtual core::Time scalar_as_time(const char* desc) const;

    virtual unsigned list_size(const char* desc) const;
    virtual bool list_as_bool(unsigned idx, const char* desc) const;
    virtual long long int list_as_int(unsigned idx, const char* desc) const;
    virtual double list_as_double(unsigned idx, const char* desc) const;
    virtual std::string list_as_string(unsigned idx, const char* desc) const;
    virtual std::unique_ptr<types::Type> list_as_type(unsigned idx, const char* desc, const structured::Keys& keys) const;
    virtual void list_sub(unsigned idx, const char* desc, std::function<void(const Reader&)>) const;

    virtual bool dict_has_key(const std::string& key, NodeType type) const;
    virtual bool dict_as_bool(const std::string& key, const char* desc) const;
    virtual long long int dict_as_int(const std::string& key, const char* desc) const;
    virtual double dict_as_double(const std::string& key, const char* desc) const;
    virtual std::string dict_as_string(const std::string& key, const char* desc) const;
    virtual core::Time dict_as_time(const std::string& key, const char* desc) const;
    virtual std::unique_ptr<types::Type> dict_as_type(const std::string& key, const char* desc, const structured::Keys& keys) const;
    virtual void dict_items(const char* desc, std::function<void(const std::string&, const Reader&)>) const;
    virtual void dict_sub(const std::string& key, const char* desc, std::function<void(const Reader&)>) const;

    bool as_bool(const char* desc) const { return scalar_as_bool(desc); }
    bool as_bool(unsigned idx, const char* desc) const { return list_as_bool(idx, desc); }
    bool as_bool(const std::string& key, const char* desc) const { return dict_as_bool(key, desc); }
    long long int as_int(const char* desc) const { return scalar_as_int(desc); }
    long long int as_int(unsigned idx, const char* desc) const { return list_as_int(idx, desc); }
    long long int as_int(const std::string& key, const char* desc) const { return dict_as_int(key, desc); }
    double as_double(const char* desc) const { return scalar_as_double(desc); }
    double as_double(unsigned idx, const char* desc) const { return list_as_double(idx, desc); }
    double as_double(const std::string& key, const char* desc) const { return dict_as_double(key, desc); }
    std::string as_string(const char* desc) const { return scalar_as_string(desc); }
    std::string as_string(unsigned idx, const char* desc) const { return list_as_string(idx, desc); }
    std::string as_string(const std::string& key, const char* desc) const { return dict_as_string(key, desc); }
    core::Time as_time(const char* desc) const { return scalar_as_time(desc); }
    core::Time as_time(const std::string& key, const char* desc) const { return dict_as_time(key, desc); }
    std::unique_ptr<types::Type> as_type(unsigned idx, const char* desc, const structured::Keys& keys) const { return list_as_type(idx, desc, keys); }
    std::unique_ptr<types::Type> as_type(const std::string& key, const char* desc, const structured::Keys& keys) const { return dict_as_type(key, desc, keys); }
    void sub(unsigned idx, const char* desc, std::function<void(const Reader&)> f) const { list_sub(idx, desc, f); }
    void sub(const std::string& key, const char* desc, std::function<void(const Reader&)> f) const { dict_sub(key, desc, f); }
    bool has_key(const std::string& key, NodeType type) const { return dict_has_key(key, type); }
    void items(const char* desc, std::function<void(const std::string&, const Reader&)> f) const { dict_items(desc, f); }
};

}
}

#endif
