#ifndef ARKI_SUMMARY_SHORT_H
#define ARKI_SUMMARY_SHORT_H

#include <arki/summary.h>
#include <arki/summary/stats.h>
#include <arki/types/typeset.h>
#include <map>

namespace arki {
struct Formatter;
struct Emitter;

namespace summary {

struct Short : public summary::Visitor
{
    std::map<types::Code, types::TypeSet> items;
    summary::Stats stats;

    /// Serialise using an emitter
    void serialise(Emitter& e, const Formatter* f=0) const;

    /**
     * Write the short summary as YAML text to the given output stream.
     */
    void write_yaml(std::ostream& out, const Formatter* f=nullptr) const;

    bool operator()(const std::vector<const types::Type*>& md, const summary::Stats& stats) override;
};

}
}
#endif
