#ifndef ARKI_SUMMARY_TABLE_H
#define ARKI_SUMMARY_TABLE_H

/// summary/table - Arkimet summary implementation in tabular form

#include <arki/types.h>
#include <arki/core/fwd.h>
#include <arki/structured/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/matcher/fwd.h>
#include <arki/summary/stats.h>
#include <vector>
#include <array>
#include <cstring>

namespace arki {
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
    std::array<const types::Type*, mso_size> items;
    Stats stats;

    Row() {}
    Row(const Row& row) = default;
    Row(Row&& row) = default;
    Row(const Metadata& md) : stats(Stats(md)) {}
    Row(const Stats& stats) : stats(stats) {}

    Row& operator=(const Row&) = default;
    Row& operator=(Row&&) = default;

    void set_to_zero()
    {
        items.fill(nullptr);
    }

    void set_to_zero(unsigned first_el)
    {
        for (unsigned i = first_el; i < items.size(); ++i)
            items[i] = nullptr;
    }

    bool matches(const Matcher& matcher) const;

    bool operator<(const Row& row) const { return items < row.items; }
    bool operator==(const Row& row) const { return items == row.items; }
    bool operator!=(const Row& row) const { return items != row.items; }

    void dump(std::ostream& out, unsigned indent = 0) const;
};

class Table
{
protected:
    TypeIntern* interns;
    std::vector<Row> rows;
    unsigned dirty = 0;

    static void buildMsoSerLen();
    static void buildItemMsoMap();

    void want_clean();

public:
    /// Aggregated global statistics
    Stats stats;

    Table();
    ~Table();

    bool empty() const { return rows.empty(); }
    size_t size() { want_clean(); return rows.size(); }

    bool equals(Table& table);

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
    void merge(const structured::Keys& keys, const structured::Reader& val);

    /// Merge a row into the table
    void merge(const Row& row);

    /// Merge rows read from a yaml input stream
    bool merge_yaml(core::LineReader& in, const std::string& filename);

    void dump(std::ostream& out);

    // Notifies the visitor of all the values of the given metadata item.
    // Due to the internal structure, the same item can be notified more than once.
    bool visitItem(size_t msoidx, ItemVisitor& visitor);

    /**
     * Visit all the contents of this node, notifying visitor of all the full
     * nodes found
     */
    bool visit(Visitor& visitor);

    /**
     * Visit all the contents of this node, notifying visitor of all the full
     * nodes found that match the matcher
     */
    bool visitFiltered(const Matcher& matcher, Visitor& visitor);

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
