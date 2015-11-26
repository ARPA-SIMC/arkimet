#ifndef ARKI_SUMMARY_CODEC_H
#define ARKI_SUMMARY_CODEC_H

/// Summary I/O implementation

#include <arki/types.h>
#include <arki/summary.h>
#include <vector>

namespace arki {
class Metadata;
class Matcher;

namespace summary {
struct Stats;
struct Visitor;
struct StatsVisitor;
struct ItemVisitor;
struct Table;

size_t decode(const std::vector<uint8_t>& buf, unsigned version, const std::string& filename, Table& target);

struct EncodingVisitor : public Visitor
{
    // Encoder we send data to
    utils::codec::Encoder& enc;

    // Last metadata encoded so far
    std::vector<const types::Type*> last;

    EncodingVisitor(utils::codec::Encoder& enc);

    bool operator()(const std::vector<const types::Type*>& md, const Stats& stats) override;
};

}
}
#endif
