#ifndef ARKI_EMITTER_JSON_H
#define ARKI_EMITTER_JSON_H

/// Emitter implementation that outputs JSON

#include <arki/core/fwd.h>
#include <arki/structured/emitter.h>
#include <iosfwd>
#include <vector>

namespace arki {
namespace structured {

/**
 * JSON emitter
 */
class JSON : public Emitter
{
protected:
    enum State {
        LIST_FIRST,
        LIST,
        MAPPING_KEY_FIRST,
        MAPPING_KEY,
        MAPPING_VAL,
    };
    std::ostream& out;
    std::vector<State> stack;

    void val_head();

public:
    JSON(std::ostream& out);
    virtual ~JSON();

    void start_list() override;
    void end_list() override;

    void start_mapping() override;
    void end_mapping() override;

    void add_null() override;
    void add_bool(bool val) override;
    void add_int(long long int val) override;
    void add_double(double val) override;
    void add_string(const std::string& val) override;

    void add_break() override;
    void add_raw(const std::string& val) override;
    void add_raw(const std::vector<uint8_t>& val) override;

    static void parse(arki::core::BufferedReader& in, Emitter& e);
    static void parse(const std::string& str, Emitter& e);
};

} // namespace structured
} // namespace arki
#endif
