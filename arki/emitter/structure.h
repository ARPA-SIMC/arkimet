#ifndef ARKI_EMITTER_STRUCTURE_H
#define ARKI_EMITTER_STRUCTURE_H

#include <arki/core/fwd.h>
#include <arki/emitter/fwd.h>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <functional>

namespace arki {
namespace emitter {

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


struct Reader
{
    virtual NodeType type() const = 0;

    virtual bool as_bool(const char* desc) const;
    virtual long long int as_int(const char* desc) const;
    virtual double as_double(const char* desc) const;
    virtual std::string as_string(const char* desc) const;

    virtual unsigned list_size(const char* desc) const;
    virtual bool as_bool(unsigned idx, const char* desc) const;
    virtual long long int as_int(unsigned idx, const char* desc) const;
    virtual double as_double(unsigned idx, const char* desc) const;
    virtual std::string as_string(unsigned idx, const char* desc) const;
    virtual void sub(unsigned idx, const char* desc, std::function<void(const Reader&)>) const;

    virtual bool has_key(const std::string& key, NodeType type) const;
    virtual bool as_bool(const std::string& key, const char* desc) const;
    virtual long long int as_int(const std::string& key, const char* desc) const;
    virtual double as_double(const std::string& key, const char* desc) const;
    virtual std::string as_string(const std::string& key, const char* desc) const;
    virtual core::Time as_time(const std::string& key, const char* desc) const;
    virtual void items(const char* desc, std::function<void(const std::string&, const Reader&)>) const;
    virtual void sub(const std::string& key, const char* desc, std::function<void(const Reader&)>) const;
};

}
}

#endif
