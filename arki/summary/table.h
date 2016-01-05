#ifndef ARKI_SUMMARY_TABLE_H
#define ARKI_SUMMARY_TABLE_H

/// summary/table - Arkimet summary implementation in tabular form

#include <arki/types.h>
#include <arki/file.h>
#include <arki/summary/stats.h>
#include <cstring>

namespace arki {
class Metadata;
class Matcher;

namespace emitter {
namespace memory {
class Mapping;
}
}

namespace summary {
class TypeIntern;
class Stats;
struct Visitor;
struct StatsVisitor;
struct ItemVisitor;

struct Row
{
    /// Number of item types that contribute to a summary context (same as Table::msoSize)
    static const unsigned mso_size = 10;
    const types::Type* items[mso_size];
    Stats stats;

    Row() {}
    Row(const Row& row)
        : stats(row.stats)
    {
        memcpy(items, row.items, sizeof(items));
    }
    Row(const Metadata& md) : stats(Stats(md)) {}
    Row(const Stats& stats) : stats(stats) {}

    void set_to_zero()
    {
        memset(items, 0, sizeof(items));
    }

    void set_to_zero(unsigned first_el)
    {
        memset(items + first_el, 0, (mso_size - first_el) * sizeof(const types::Type*));
    }

    bool matches(const Matcher& matcher) const;

    bool operator<(const Row& row) const;

    bool operator==(const Row& row) const
    {
        return memcmp(items, row.items, sizeof(items)) == 0;
    }

    bool operator!=(const Row& row) const
    {
        return memcmp(items, row.items, sizeof(items)) != 0;
    }

    void dump(std::ostream& out, unsigned indent = 0) const;

private:
    Row& operator=(const Row&);
};

class Table
{
protected:
    TypeIntern* interns;
    Row* rows;
    unsigned row_count;
    unsigned row_capacity;

    /// Ensure that we have enough capacity to add at last an item
    void ensure_we_can_add_one();

    static void buildMsoSerLen();
    static void buildItemMsoMap();

public:
    /// Aggregated global statistics
    Stats stats;

    Table();
    ~Table();

    bool empty() const { return row_count == 0; }
    unsigned size() const { return row_count; }

    bool equals(const Table& table) const;

    /// Return the intern version of an item
    const types::Type* intern(unsigned pos, std::unique_ptr<types::Type>&& item);

    /// Merge a row into the table
    void merge(const Metadata& md);

    /// Merge a row into the table
    void merge(const Metadata& md, const Stats& st);

    /// Merge a row into the table
    void merge(const std::vector<const types::Type*>& md, const Stats& st);

    /// Merge a row into the table, keeping only the items whose mso index is in \a positions
    void merge(const std::vector<const types::Type*>& md, const Stats& st, const std::vector<unsigned>& positions);

    /// Merge an entry decoded from a mapping
    void merge(const emitter::memory::Mapping& val);

    /// Merge a row into the table
    void merge(const Row& row);

    /// Merge rows read from a yaml input stream
    bool merge_yaml(LineReader& in, const std::string& filename);

    void dump(std::ostream& out) const;

    // Notifies the visitor of all the values of the given metadata item.
    // Due to the internal structure, the same item can be notified more than once.
    bool visitItem(size_t msoidx, ItemVisitor& visitor) const;

    /**
     * Visit all the contents of this node, notifying visitor of all the full
     * nodes found
     */
    bool visit(Visitor& visitor) const;

    /**
     * Visit all the contents of this node, notifying visitor of all the full
     * nodes found that match the matcher
     */
    bool visitFiltered(const Matcher& matcher, Visitor& visitor) const;

    /// Number of item types that contribute to a summary context
    static const size_t msoSize;
    /// Metadata Scan Order
    static const types::Code mso[];
    static int* msoSerLen;

private:
    Table(const Table&);
    Table& operator=(const Table&);
};

}
}

#endif
