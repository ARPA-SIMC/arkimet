#include "table.h"
#include "intern.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/matcher/utils.h"
#include "arki/types/utils.h"
#include "arki/structured/keys.h"
#include "arki/structured/reader.h"
#include "arki/summary.h"
#include "arki/utils/yaml.h"
#include <system_error>
#include <algorithm>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace summary {

// Metadata Scan Order
const types::Code Table::mso[] = {
        TYPE_ORIGIN,
        TYPE_PRODUCT,
        TYPE_LEVEL,
        TYPE_TIMERANGE,
        TYPE_AREA,
        TYPE_PRODDEF,
        TYPE_BBOX,
        TYPE_RUN,
        TYPE_QUANTITY,
        TYPE_TASK

};
const size_t Table::msoSize = sizeof(mso) / sizeof(Code);
int* Table::msoSerLen = 0;

// Reverse mapping
static int* itemMsoMap = 0;
static size_t itemMsoMapSize = 0;

bool Row::matches(const Matcher& matcher) const
{
    if (matcher.empty()) return true;

    for (unsigned i = 0; i < mso_size; ++i)
    {
        shared_ptr<matcher::OR> item_matcher = matcher.get(Table::mso[i]);
        if (!item_matcher) continue;
        if (!items[i]) return false;
        if (!item_matcher->matchItem(*items[i])) return false;
    }
    shared_ptr<matcher::OR> reftime_matcher = matcher.get(TYPE_REFTIME);
    if (reftime_matcher && !reftime_matcher->matchItem(*stats.make_reftime()))
        return false;
    return true;
}

void Row::dump(std::ostream& out, unsigned indent) const
{
    string head(indent, ' ');

    size_t max_tag = 0;
    for (unsigned i = 0; i < mso_size; ++i)
        max_tag = max(max_tag, types::tag(Table::mso[i]).size());

    for (unsigned i = 0; i < mso_size; ++i)
    {
        string tag = types::tag(Table::mso[i]);
        out << head;
        for (unsigned j = 0; j < max_tag - tag.size(); ++j)
            out << ' ';
        out << tag << " ";
        if (items[i])
            out << *items[i] << endl;
        else
            out << "--" << endl;
    }
}

Table::Table()
    : interns(new TypeIntern[Table::msoSize])
{
    buildMsoSerLen();
    buildItemMsoMap();
}

Table::~Table()
{
    delete[] interns;
}

void Table::want_clean()
{
    if (dirty == 0) return;

    // Sort the rows
    std::sort(rows.begin(), rows.end());

    // If the table has only one item, it is trivially sorted and unique
    if (rows.size() == 1)
    {
        dirty = 0;
        return;
    }

    // Merge consecutive rows with the same metadata
    vector<Row>::iterator read = rows.begin() + 1;
    vector<Row>::iterator write = rows.begin();

    while (true)
    {
        if (read == rows.end()) break;
        if (*read == *write)
        {
            write->stats.merge(read->stats);
            ++read;
            continue;
        } else {
            ++write;
            if (read != write)
                *write = *read;
            ++read;
        }
    }
    rows.resize(write - rows.begin() + 1);
    dirty = 0;
}

bool Table::equals(Table& table)
{
    want_clean();
    table.want_clean();

    if (rows.size() != table.rows.size()) return false;
    for (unsigned ri = 0; ri < rows.size(); ++ri)
    {
        Row translated(table.rows[ri].stats);
        // Translate the row in table to the pointer that we use in *this
        for (unsigned ci = 0; ci < msoSize; ++ci)
        {
            if (table.rows[ri].items[ci] == 0)
                translated.items[ci] = 0;
            else
            {
                translated.items[ci] = interns[ci].lookup(*table.rows[ri].items[ci]);
                if (!translated.items[ci]) return false;
            }
        }

        // Lookup translated in this table
        auto pos = lower_bound(rows.begin(), rows.end(), translated);
        if (pos == rows.end()) return false;
        if (*pos != translated) return false;
        if (pos->stats != translated.stats) return false;
    }
    return true;
}

const types::Type* Table::intern(unsigned pos, std::unique_ptr<types::Type>&& item)
{
    return interns[pos].intern(move(item));
}

void Table::merge(const Metadata& md)
{
    merge(md, Stats(md));
}

void Table::merge(const Metadata& md, const Stats& st)
{
    Row new_row(st);
    for (size_t i = 0; i < msoSize; ++i)
    {
        const Type* item = md.get(mso[i]);
        if (item)
            new_row.items[i] = interns[i].intern(*item);
        else
            new_row.items[i] = 0;
    }

    merge(new_row);
}

void Table::merge(const std::vector<const Type*>& md, const Stats& st)
{
    Row new_row(st);
    for (size_t i = 0; i < msoSize; ++i)
    {
        if (i < md.size() && md[i])
            new_row.items[i] = interns[i].intern(*md[i]);
        else
            new_row.items[i] = 0;
    }

    merge(new_row);
}

void Table::merge(const std::vector<const types::Type*>& md, const Stats& st, const vector<unsigned>& positions)
{
    Row new_row(st);
    new_row.set_to_zero();
    for (vector<unsigned>::const_iterator i = positions.begin(); i != positions.end(); ++i)
    {
        if (*i < md.size() && md[*i])
            new_row.items[*i] = interns[*i].intern(*md[*i]);
        else
            new_row.items[*i] = 0;
    }

    merge(new_row);
}

void Table::merge(const structured::Keys& keys, const structured::Reader& val)
{
    using namespace structured::memory;

    unique_ptr<summary::Stats> stats;
    val.sub(keys.summary_stats, "summary stats", [&](const structured::Reader& summarystats) {
        stats = summary::Stats::decode_structure(keys, summarystats);
    });
    Row new_row(*stats);
    new_row.set_to_zero();

    for (size_t pos = 0; pos < msoSize; ++pos)
    {
        types::Code code = summary::Visitor::codeForPos(pos);
        if (!val.has_key(types::tag(code), structured::NodeType::MAPPING))
            continue;
        val.sub(types::tag(code), "summary item", [&](const structured::Reader& item) {
            new_row.items[pos] = interns[pos].intern(types::decode_structure(keys, code, item));
        });
    }

    merge(new_row);
}

bool Table::merge_yaml(LineReader& in, const std::string& filename)
{
    Row new_row;
    new_row.set_to_zero();
    YamlStream yamlStream;
    for (YamlStream::const_iterator i = yamlStream.begin(in); i != yamlStream.end(); ++i)
    {
        types::Code type = types::parseCodeName(i->first);
        switch (type)
        {
            case TYPE_SUMMARYITEM:
                {
                    auto in = LineReader::from_chars(i->second.data(), i->second.size());
                    YamlStream yamlStream;
                    for (YamlStream::const_iterator i = yamlStream.begin(*in); i != yamlStream.end(); ++i)
                    {
                        types::Code type = types::parseCodeName(i->first);
                        int pos = summary::Visitor::posForCode(type);
                        if (pos < 0)
                            throw runtime_error("cannot parse summary item: found element of unsupported type " + types::formatCode(type));
                        new_row.items[pos] = interns[pos].intern(types::decodeString(type, i->second));
                    }
                }
                break;
            case TYPE_SUMMARYSTATS:
            {
                new_row.stats = *Stats::decodeString(i->second);
                merge(new_row);
                new_row.set_to_zero();
                break;
            }
            default:
            {
                stringstream ss;
                ss << "cannot parse file " << filename << ": cannot handle element " << type;
                throw std::runtime_error(ss.str());
            }
        }
    }
    return !in.eof();
}

void Table::merge(const Row& row)
{
    // Occasionally clean the table in case we are adding a lot of metadata one
    // by one, to prevent the intermediate table from exploding in size
    if (dirty > 100000)
        want_clean();
    rows.emplace_back(row);
    stats.merge(row.stats);
    ++dirty;
}

void Table::dump(std::ostream& out)
{
    want_clean();
}

bool Table::visitItem(size_t msoidx, ItemVisitor& visitor)
{
    want_clean();

    const TypeIntern& intern = interns[msoidx];
    for (TypeIntern::const_iterator i = intern.begin(); i != intern.end(); ++i)
        if (!visitor(**i))
            return false;
    return true;
}

bool Table::visit(Visitor& visitor)
{
    want_clean();

    vector<const Type*> visitmd;
    visitmd.resize(msoSize);

    for (const auto& row: rows)
    {
        // Set this node's metadata in visitmd
        // FIXME: change the visitor API to just get a const Type* const* and
        //        assume it's msoSize long
        for (size_t i = 0; i < msoSize; ++i)
            visitmd[i] = row.items[i];

        if (!visitor(visitmd, row.stats))
            return false;
    }

    return true;
}

bool Table::visitFiltered(const Matcher& matcher, Visitor& visitor)
{
    want_clean();

    vector<const Type*> visitmd;
    visitmd.resize(msoSize);

    for (const auto& row: rows)
    {
        if (!row.matches(matcher)) continue;

        // FIXME: change the visitor API to just get a const Type* const* and
        //        assume it's msoSize long
        for (size_t i = 0; i < msoSize; ++i)
            visitmd[i] = row.items[i];

        if (!visitor(visitmd, row.stats))
            return false;
    }

    return true;
}

void Table::buildMsoSerLen()
{
    if (msoSerLen) return;
    msoSerLen = new int[msoSize];
    for (size_t i = 0; i < msoSize; ++i)
    {
        const types::MetadataType* mdt = types::MetadataType::get(mso[i]);
        msoSerLen[i] = mdt ? mdt->serialisationSizeLen : 0;
    }
}
void Table::buildItemMsoMap()
{
    if (itemMsoMap) return;

    for (size_t i = 0; i < msoSize; ++i)
        if ((size_t)mso[i] > itemMsoMapSize)
            itemMsoMapSize = (size_t)mso[i];
    itemMsoMapSize += 1;

    itemMsoMap = new int[itemMsoMapSize];
    for (size_t i = 0; i < itemMsoMapSize; ++i)
        itemMsoMap[i] = -1;
    for (size_t i = 0; i < msoSize; ++i)
        itemMsoMap[(size_t)mso[i]] = i;
}

types::Code Visitor::codeForPos(size_t pos)
{
    return Table::mso[pos];
}

int Visitor::posForCode(types::Code code)
{
    if ((size_t)code >= itemMsoMapSize) return -1;
    return itemMsoMap[(size_t)code];
}

}
}
