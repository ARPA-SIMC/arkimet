#ifndef ARKI_EMITTER_H
#define ARKI_EMITTER_H

/// Generic structured data formatter

#include <string>
#include <cstdint>
#include <vector>

namespace arki {

/**
 * Abstract interface for all emitters
 */
class Emitter
{
public:
    // Public interface
    virtual ~Emitter() {}

    // Implementation
    virtual void start_list() = 0;
    virtual void end_list() = 0;

    virtual void start_mapping() = 0;
    virtual void end_mapping() = 0;

    virtual void add_null() = 0;
    virtual void add_bool(bool val) = 0;
    virtual void add_int(long long int val) = 0;
    virtual void add_double(double val) = 0;
    virtual void add_string(const std::string& val) = 0;

    /// Add a break in the output stream, such as a newline between JSON chunks
    virtual void add_break();

    /// Add raw data
    virtual void add_raw(const std::string& val);

    /// Add raw data
    virtual void add_raw(const std::vector<uint8_t>& val);

    // Shortcuts
    void add(const std::string& val) { add_string(val); }
    void add(const char* val) { add_string(val); }
    void add(double val) { add_double(val); }
    void add(int32_t val) { add_int(val); }
    void add(int64_t val) { add_int(val); }
    void add(uint32_t val) { add_int(val); }
    void add(uint64_t val) { add_int(val); }
    void add(bool val) { add_bool(val); }

    // Shortcut to add a mapping, which also ensure the key is a string
    template<typename T>
    void add(const std::string& a, T b)
    {
        add(a);
        add(b);
    }
};


}

// vim:set ts=4 sw=4:
#endif
