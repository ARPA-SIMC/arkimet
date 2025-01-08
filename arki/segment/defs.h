#ifndef ARKI_SEGMENT_DEFS_H
#define ARKI_SEGMENT_DEFS_H

#include <cstdint>
#include <iosfwd>
#include <tuple>

namespace arki::segment {

/**
 * State of a segment
 */
struct State
{
    unsigned value;

    State() : value(0) {}
    explicit State(unsigned value) : value(value) {}

    bool is_ok() const { return value == 0; }

    bool has(const State& state) const
    {
        return value & state.value;
    }

    State& operator+=(const State& fs)
    {
        value |= fs.value;
        return *this;
    }

    State& operator-=(const State& fs)
    {
        value &= ~fs.value;
        return *this;
    }

    State operator+(const State& fs) const
    {
        return State(value | fs.value);

    }

    State operator-(const State& fs) const
    {
        return State(value & ~fs.value);

    }

    bool operator==(const State& fs) const
    {
        return value == fs.value;
    }

    /// Return a text description of this file state
    std::string to_string() const;
};

static const State SEGMENT_OK(0);
static const State SEGMENT_DIRTY(1 << 0); /// Segment contains data deleted or out of order
static const State SEGMENT_UNALIGNED(1 << 1); /// Segment contents are inconsistent with the index
static const State SEGMENT_MISSING(1 << 2); /// Segment is known to the index, but does not exist on disk
static const State SEGMENT_DELETED(1 << 3); /// Segment contents have been entirely deleted
static const State SEGMENT_CORRUPTED(1 << 4); /// File is broken in a way that needs manual intervention
static const State SEGMENT_ARCHIVE_AGE(1 << 5); /// File is old enough to be archived
static const State SEGMENT_DELETE_AGE(1 << 6); /// File is old enough to be deleted


/// Print to ostream
std::ostream& operator<<(std::ostream&, const State&);

struct Span
{
    size_t offset;
    size_t size;
    Span() = default;
    Span(size_t offset, size_t size) : offset(offset), size(size) {}
    bool operator<(const Span& o) const { return std::tie(offset, size) < std::tie(o.offset, o.size); }
};

}

#endif

