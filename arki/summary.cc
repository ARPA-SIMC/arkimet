/*
 * summary - Handle a summary of a group of summary
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/summary.h>
#include <arki/summary/node.h>
#include <arki/summary/stats.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils/codec.h>
#include <arki/formatter.h>
#include <arki/types/utils.h>
#include <arki/types/area.h>
#include <arki/types/time.h>
#include <arki/utils/geosdef.h>
#include <arki/utils/compress.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
// #include <arki/utils/lua.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>

#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils::codec;

namespace arki {

Summary::Summary()
    : root(0)
{
}

Summary::Summary(const Summary& s)
    : root(0)
{
    if (s.root)
        root = s.root->clone();
}

Summary::~Summary()
{
    if (root) delete root;
}

Summary& Summary::operator=(const Summary& s)
{
    if (root != s.root)
    {
        if (root) delete root;
        if (s.root)
            root = s.root->clone();
        else
            root = 0;
    }
    return *this;
}

#ifdef HAVE_LUA
namespace {

struct LuaPusher: public summary::Visitor
{
    lua_State* L;
    int index;

    LuaPusher(lua_State* L) : L(L), index(1) {}
    virtual bool operator()(const std::vector< UItem<> >& md, const UItem<summary::Stats>& stats)
    {
        // Table with the couple
        lua_newtable(L);
        // Table with the items
        lua_newtable(L);
        // Push the items
        for (size_t i = 0; i < md.size(); ++i)
        {
            // Name
            if (md[i].defined())
            {
                // Key
                string name = str::tolower(types::formatCode(codeForPos(i)));
                lua_pushlstring(L, name.data(), name.size());
                // Value
                md[i]->lua_push(L);
                lua_rawset(L, -3);
            }
        }
        lua_rawseti(L, -2, 1);
        // Push the stats
        stats->lua_push(L);
        lua_rawseti(L, -2, 2);
        // Push the couple into the table we are populating
        lua_rawseti(L, -2, index++);
        return true;
    }
};

}

typedef utils::lua::ManagedUD<Summary> SummaryUD;

static void arkilua_getmetatable(lua_State* L);

static int arkilua_count(lua_State* L)
{
    Summary* s = Summary::lua_check(L, 1);
    luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
    lua_pushinteger(L, s->count());
    return 1;
}

static int arkilua_size(lua_State* L)
{
    Summary* s = Summary::lua_check(L, 1);
    luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
    lua_pushinteger(L, s->size());
    return 1;
}

static int arkilua_data(lua_State* L)
{
    Summary* s = Summary::lua_check(L, 1);
    luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
    // Return a big table with a dump of the summary inside
    lua_newtable(L);
    LuaPusher pusher(L);
    s->visit(pusher);
    return 1;
}

static int arkilua_filter(lua_State* L)
{
    // utils::lua::dumpstack(L, "FILTER", cerr);
    Summary* s = Summary::lua_check(L, 1);
    luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
    Matcher m = Matcher::lua_check(L, 2);
    if (lua_gettop(L) > 2)
    {
        // s.filter(matcher, s1)
        Summary* s1 = Summary::lua_check(L, 3);
        luaL_argcheck(L, s1 != NULL, 3, "`arki.summary' expected");
        s->filter(m, *s1);
        return 0;
    } else {
        SummaryUD::create(L, new Summary(s->filter(m)), true);
        return 1;
    }
}

// Make a new summary
// Memory management of the copy will be done by Lua
static int arkilua_new(lua_State* L)
{
    // Make a new copy
    SummaryUD::create(L, new Summary, true);

    // Set the summary for the userdata
    arkilua_getmetatable(L);
    lua_setmetatable(L, -2);

    return 1;
}

// Make a copy of the metadata.
// Memory management of the copy will be done by Lua
static int arkilua_copy(lua_State* L)
{
    Summary* s = Summary::lua_check(L, 1);
    luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");

    // Make a new copy
    SummaryUD* ud = SummaryUD::create(L, new Summary, true);
    *(ud->val) = *s;

    // Set the summary for the userdata
    arkilua_getmetatable(L);
    lua_setmetatable(L, -2);

    return 1;
}


static int arkilua_gc (lua_State *L)
{
    SummaryUD* ud = (SummaryUD*)luaL_checkudata(L, 1, "arki.summary");
    if (ud != NULL && ud->collected)
        delete ud->val;
    return 0;
}

static int arkilua_tostring (lua_State *L)
{
    lua_pushstring(L, "summary");
    return 1;
}


#if 0
    else if (name == "iter")
    {
        // Iterate

        /* create a userdatum to store an iterator structure address */
        summary::LuaIter**d = (summary::LuaIter**)lua_newuserdata(L, sizeof(summary::LuaIter*));

        // Get the metatable for the iterator
        if (luaL_newmetatable(L, "arki_summary_iter"));
        {
            /* set its __gc field */
            lua_pushstring(L, "__gc");
            lua_pushcfunction(L, summary::LuaIter::gc);
            lua_settable(L, -3);
        }

        // Set its metatable
        lua_setmetatable(L, -2);

        // Instantiate the iterator
        *d = new summary::LuaIter(v);

        // Creates and returns the iterator function (its sole upvalue, the
        // iterator userdatum, is already on the stack top
        lua_pushcclosure(L, summary::LuaIter::iterate, 1);
        return 1;
    }
#endif

static const struct luaL_reg summaryclasslib [] = {
    { "new", arkilua_new },
    { NULL, NULL }
};

static const struct luaL_reg summarylib [] = {
    { "count", arkilua_count },
    { "size", arkilua_size },
    { "data", arkilua_data },
    { "filter", arkilua_filter },
    { "copy", arkilua_copy },
    { "__gc", arkilua_gc },
    { "__tostring", arkilua_tostring },
    { NULL, NULL }
};

static void arkilua_getmetatable(lua_State* L)
{
    // Set the metatable for the userdata
    if (luaL_newmetatable(L, "arki.summary"));
    {
        // If the metatable wasn't previously created, create it now
        lua_pushstring(L, "__index");
        lua_pushvalue(L, -2);  /* pushes the metatable */
        lua_settable(L, -3);  /* metatable.__index = metatable */

        // Load normal methods
        luaL_register(L, NULL, summarylib);
    }
}

void Summary::lua_push(lua_State* L)
{
    SummaryUD::create(L, this, false);
    arkilua_getmetatable(L);
    lua_setmetatable(L, -2);
}

void Summary::lua_openlib(lua_State* L)
{
    luaL_register(L, "arki.summary", summaryclasslib);
}

Summary* Summary::lua_check(lua_State* L, int idx)
{
    SummaryUD* ud = (SummaryUD*)luaL_checkudata(L, idx, "arki.summary");
    if (ud) return ud->val;
    return NULL;
}
#endif

bool Summary::operator==(const Summary& m) const
{
    if (root == 0 && m.root == 0) return true;
    if (root == 0 && m.root != 0) return false;
    if (root != 0 && m.root == 0) return false;
    return root->compare(*m.root) == 0;
}

void Summary::clear()
{
    if (root) delete root;
    root = 0;
}

namespace summary {
struct StatsCount : public StatsVisitor
{
    size_t count;
    StatsCount() : count(0) {}
    virtual bool operator()(const Stats& stats)
    {
        count += stats.count;
        return true;
    }
};
struct StatsSize : public StatsVisitor
{
    unsigned long long size;
    StatsSize() : size(0) {}
    virtual bool operator()(const Stats& stats)
    {
        size += stats.size;
        return true;
    }
};
struct StatsReftime : public StatsVisitor
{
    types::reftime::Collector merger;

    virtual bool operator()(const Stats& stats)
    {
        merger.merge(stats.reftimeMerger);
        return true;
    }
};
#ifdef HAVE_GEOS
struct StatsHull : public ItemVisitor
{
    ARKI_GEOS_GEOMETRYFACTORY& gf;
    vector<ARKI_GEOS_GEOMETRY*>* geoms;
    std::set< Item<types::Area> > seen;

    StatsHull(ARKI_GEOS_GEOMETRYFACTORY& gf) : gf(gf), geoms(new vector<ARKI_GEOS_GEOMETRY*>) {}
    virtual ~StatsHull()
    {
        if (geoms)
        {
            for (vector<ARKI_GEOS_GEOMETRY*>::iterator i = geoms->begin(); i != geoms->end(); ++i)
                delete *i;
            delete geoms;
        }
    }

    virtual bool operator()(const arki::UItem<>& area)
    {
        if (!area.defined()) return true;
        Item<types::Area> a = area.upcast<types::Area>();
        pair<set< Item<types::Area> >::iterator, bool> i = seen.insert(a);
        if (i.second)
        {
            const ARKI_GEOS_GEOMETRY* g = a->bbox();
            //cerr << "Got: " << g << g->getGeometryType() << endl;
            if (!g) return true;
            //cerr << "Adding: " << g->toString() << endl;
            geoms->push_back(g->clone());
        }
        return true;
    }

    auto_ptr<ARKI_GEOS_GEOMETRY> makeBBox()
    {
        if (geoms->empty())
            return auto_ptr<ARKI_GEOS_GEOMETRY>(0);

        auto_ptr<ARKI_GEOS_NS::GeometryCollection> gc(gf.createGeometryCollection(geoms));
        geoms = 0;
        return auto_ptr<ARKI_GEOS_GEOMETRY>(gc->convexHull());
    }
};
#endif
}

size_t Summary::count() const
{
    if (!root) return 0;
    summary::StatsCount counter;
    root->visitStats(counter);
    return counter.count;
}

unsigned long long Summary::size() const
{
    if (!root) return 0;
    summary::StatsSize counter;
    root->visitStats(counter);
    return counter.size;
}

void Summary::dump(std::ostream& out) const
{
    if (!root)
        out << "(empty summary)" << endl;
    else
        root->dump(out);
}

Item<types::Reftime> Summary::getReferenceTime() const
{
    summary::StatsReftime counter;
    if (root)
        root->visitStats(counter);
    return counter.merger.makeReftime();
}

namespace {
struct ResolveVisitor : public summary::Visitor
{
    std::vector<ItemSet>& result;
    std::vector<types::Code> codes;
    size_t added;

    ResolveVisitor(std::vector<ItemSet>& result, const Matcher& m) : result(result), added(0)
    {
        for (matcher::AND::const_iterator i = m.m_impl->begin(); i != m.m_impl->end(); ++i)
            codes.push_back(i->first);
    }
    virtual ~ResolveVisitor() {}
    virtual bool operator()(const std::vector< UItem<> >& md, const UItem<summary::Stats>& stats)
    {
        ItemSet is;
        for (std::vector<types::Code>::const_iterator i = codes.begin();
                i != codes.end(); ++i)
        {
            int pos = posForCode(*i);
            if (!md[pos].defined()) return true;
            is.set(md[pos]);
        }
        ++added;
        // Insertion sort, as we expect to have lots of duplicates
        std::vector<ItemSet>::iterator i = std::lower_bound(result.begin(), result.end(), is);
        if (i == result.end())
            result.push_back(is);
        else if (*i != is)
            result.insert(i, is);

        return true;
    }
};
}

std::vector<ItemSet> Summary::resolveMatcher(const Matcher& matcher) const
{
    if (matcher.empty()) return std::vector<ItemSet>();

    std::vector<ItemSet> result;
    ResolveVisitor visitor(result, matcher);
    visitFiltered(matcher, visitor);

    return result;
}

size_t Summary::resolveMatcher(const Matcher& matcher, std::vector<ItemSet>& res) const
{
    if (matcher.empty()) return 0;

    ResolveVisitor visitor(res, matcher);
    visitFiltered(matcher, visitor);
    return visitor.added;
}

std::auto_ptr<ARKI_GEOS_GEOMETRY> Summary::getConvexHull(ARKI_GEOS_GEOMETRYFACTORY& gf) const
{
#ifdef HAVE_GEOS
    summary::StatsHull merger(gf);
    if (root)
    {
        summary::Node::buildItemMsoMap();
        root->visitItem(summary::Visitor::posForCode(types::TYPE_AREA), merger);
    }
    return merger.makeBBox();
#else
    return std::auto_ptr<ARKI_GEOS_GEOMETRY>(0);
#endif
}


bool Summary::read(int fd, const std::string& filename)
{
    wibble::sys::Buffer buf;
    string signature;
    unsigned version;
    if (!types::readBundle(fd, filename, buf, signature, version))
        return false;

    // Ensure first 2 bytes are SU
    if (signature != "SU")
        throw wibble::exception::Consistency("parsing file " + filename, "summary entry does not start with 'SU'");

    read(buf, version, filename);

    return true;
}

bool Summary::read(std::istream& in, const std::string& filename)
{
    wibble::sys::Buffer buf;
    string signature;
    unsigned version;
    if (!types::readBundle(in, filename, buf, signature, version))
        return false;

    // Ensure first 2 bytes are SU
    if (signature != "SU")
        throw wibble::exception::Consistency("parsing file " + filename, "summary entry does not start with 'SU'");

    read(buf, version, filename);

    return true;
}

void Summary::read(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename)
{
    using namespace summary;

    clear();

    root = RootNode::decode(buf, version, filename);
}

std::string Summary::encode(bool compressed) const
{
    using namespace utils::codec;

    /*
    // Build the serialisation tables if it has not been done yet
    summary::Node::buildMsoSerLen();
    */

    // Encode
    string inner;
    Encoder innerenc(inner);
    if (root) root->encode(innerenc);

    // Prepend header
    string res;
    Encoder enc(res);
    // Signature
    enc.addString("SU", 2);
    // Version
    enc.addUInt(3, 2);
    if (compressed)
    {
        sys::Buffer comp = utils::compress::lzo(inner.data(), inner.size());
        if (comp.size() + 4 >= inner.size())
        {
            // No point in compressing
            enc.addUInt(inner.size() + 1, 4);
            // Add compression type (uncompressed)
            enc.addUInt(0, 1);
            enc.addString(inner);
        } else {
            // Compression makes sense
            // Add total size
            enc.addUInt(comp.size() + 5, 4);
            // Add compression type (LZO)
            enc.addUInt(1, 1);
            // Add uncompressed size
            enc.addUInt(inner.size(), 4);
            // Add compressed data
            enc.addBuffer(comp);
        }
    } else {
        enc.addUInt(inner.size() + 1, 4);
        enc.addUInt(0, 1);
        enc.addString(inner);
    }
    return res;
}

void Summary::write(std::ostream& out, const std::string& filename) const
{
    // Prepare the encoded data
    string encoded = encode(true);

    // Write out
    out.write(encoded.data(), encoded.size());
    if (out.fail())
        throw wibble::exception::File(filename, "writing " + str::fmt(encoded.size()) + " bytes to the file");
}

void Summary::writeAtomically(const std::string& fname)
{
    // Write summary to disk
    string enc = encode(true);
    string tmpfile = fname + ".tmp" + str::fmt(getpid());
    int fd = open(tmpfile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd == -1)
        throw wibble::exception::File(tmpfile, "creating temporary file for the summary");
    try {
        int res = ::write(fd, enc.data(), enc.size());
        if (res < 0 || (unsigned)res != enc.size())
            throw wibble::exception::File(tmpfile, "writing " + str::fmt(enc.size()) + " bytes to the file");

        if (close(fd) == -1)
        {
            fd = -1;
            throw wibble::exception::File(tmpfile, "closing file");
        }
        fd = -1;
        if (rename(tmpfile.c_str(), fname.c_str()) == -1)
            throw wibble::exception::System("Renaming " + tmpfile + " into " + fname);
    } catch (...) {
        if (fd != -1)
            close(fd);
        throw;
    }
}

namespace summary {
struct YamlPrinter : public Visitor
{
    ostream& out;
    string indent;
    const Formatter* f;

    YamlPrinter(ostream& out, size_t indent, const Formatter* f = 0) : out(out), indent(indent, ' '), f(f) {}
    virtual bool operator()(const std::vector< UItem<> >& md, const UItem<Stats>& stats)
    {
        // Write the metadata items
        out << "SummaryItem:" << endl;
        for (vector< UItem<> >::const_iterator i = md.begin(); i != md.end(); ++i)
        {
            if (!i->defined()) continue;

            out << indent << str::ucfirst((*i)->tag()) << ": ";
            (*i)->writeToOstream(out);
            if (f) out << "\t# " << (*f)(*i);
            out << endl;
        }

        // Write the stats
        out << "SummaryStats:" << endl;
        arki::Item<types::Reftime> reftime(stats->reftimeMerger.makeReftime());
        out << indent << "Count: " << stats->count << endl;
        out << indent << "Size: " << stats->size << endl;
        out << indent << "Reftime: " << reftime << endl;
        return true;
    }
};
}

bool Summary::visit(summary::Visitor& visitor) const
{
    if (!root) return true;
    return root->visit(visitor);
}

bool Summary::visitFiltered(const Matcher& matcher, summary::Visitor& visitor) const
{
    if (!root) return true;
    return root->visitFiltered(matcher, visitor);
}

void Summary::writeYaml(std::ostream& out, const Formatter* f) const
{
    if (!root) return;
    summary::YamlPrinter printer(out, 2, f);
    visit(printer);
}

void Summary::serialise(Emitter& e, const Formatter* f) const
{
    e.start_mapping();
    e.add("items");
    e.start_list();
    if (root)
    {
        struct Serialiser : public summary::Visitor
        {
            Emitter& e;
            const Formatter* f;

            Serialiser(Emitter& e, const Formatter* f) : e(e), f(f) {}

            virtual bool operator()(const std::vector< UItem<> >& md, const UItem<summary::Stats>& stats)
            {
                e.start_mapping();
                for (std::vector< UItem<> >::const_iterator i = md.begin();
                        i != md.end(); ++i)
                {
                    if (!i->defined()) continue;
                    e.add((*i)->tag());
                    e.start_mapping();
                    (*i)->serialiseLocal(e, f);
                    e.end_mapping();
                }
                e.add(stats->tag());
                e.start_mapping();
                stats->serialiseLocal(e, f);
                e.end_mapping();
                e.end_mapping();
                return true;
            }
        } visitor(e, f);

        visit(visitor);
    }
    e.end_list();
    e.end_mapping();
}

void Summary::read(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    const List& items = val["items"].want_list("parsing summary item list");
    for (std::vector<const Node*>::const_iterator i = items.val.begin(); i != items.val.end(); ++i)
    {
        const Mapping& m = (*i)->want_mapping("parsing summary item");
        std::vector< UItem<> > md;
        for (size_t pos = 0; pos < summary::Node::msoSize; ++pos)
        {
            types::Code code = summary::Visitor::codeForPos(pos);
            const Node& n = m[types::tag(code)];
            if (n.is_mapping())
            {
                md.resize(pos + 1);
                md[pos] = types::decodeMapping(code, n.get_mapping());
            }
        }

        UItem<summary::Stats> stats = summary::Stats::decodeMapping(
                m["summarystats"].want_mapping("parsing summary item stats"));
        if (root)
            root->add(md, stats);
        else
            root = new summary::RootNode(md, stats);
    }
}

static vector< UItem<> > decodeItem(const std::string& str)
{
    using namespace str;

    vector< UItem<> > itemmd;
    stringstream in(str, ios_base::in);
    YamlStream yamlStream;
    for (YamlStream::const_iterator i = yamlStream.begin(in);
            i != yamlStream.end(); ++i)
    {
        types::Code type = types::parseCodeName(i->first);
        int pos = summary::Visitor::posForCode(type);
        if (pos < 0)
            throw wibble::exception::Consistency("parsing summary item", "found element of unsupported type " + types::formatCode(type));
        if ((size_t)pos >= itemmd.size())
            itemmd.resize(pos + 1);
        itemmd[pos] = types::decodeString(type, i->second);
    }
    return itemmd;
}

void Summary::readFile(const std::string& fname)
{
    // Read all the metadata
    std::ifstream in;
    in.open(fname.c_str(), ios::in);
    if (!in.is_open() || in.fail())
        throw wibble::exception::File(fname, "opening file for reading");

    read(in, fname);

    in.close();
}

bool Summary::readYaml(std::istream& in, const std::string& filename)
{
    using namespace summary;
    using namespace str;

    summary::Node::buildItemMsoMap();

    vector< UItem<> > itemmd;
    YamlStream yamlStream;
    for (YamlStream::const_iterator i = yamlStream.begin(in);
            i != yamlStream.end(); ++i)
    {
        types::Code type = types::parseCodeName(i->first);
        switch (type)
        {
            case types::TYPE_SUMMARYITEM: itemmd = decodeItem(i->second); break;
            case types::TYPE_SUMMARYSTATS:
            {
                if (root)
                    root->add(itemmd, Stats::decodeString(i->second));
                else
                    root = new RootNode(itemmd, Stats::decodeString(i->second));
                break;
            }
            default:
                throw wibble::exception::Consistency("parsing file " + filename,
                    "cannot handle element " + fmt(type));
        }
    }
    return !in.eof();
}

void Summary::add(const Metadata& md)
{
    if (root)
        root->add(md);
    else
        root = new summary::RootNode(md);
}

void Summary::add(const Metadata& md, const UItem<summary::Stats>& s)
{
    if (root)
        root->add(md, s);
    else
        root = new summary::RootNode(md, s);
}

namespace summary {
struct SummaryMerger : public Visitor
{
    RootNode*& root;

    SummaryMerger(RootNode*& root) : root(root) {}
    virtual bool operator()(const std::vector< UItem<> >& md, const UItem<Stats>& stats)
    {
        if (!root)
            root = new RootNode(md, stats);
        else
            root->add(md, stats);
        return true;
    }
};
struct PruningSummaryMerger : public Visitor
{
    const set<types::Code> keep_only;
    RootNode*& root;

    PruningSummaryMerger(const set<types::Code> keep_only, RootNode*& root)
        : keep_only(keep_only), root(root) {}

    virtual bool operator()(const std::vector< UItem<> >& md, const UItem<Stats>& stats)
    {
        std::vector< UItem<> > md1;
        md1.resize(md.size());
        for (set<types::Code>::const_iterator i = keep_only.begin();
                i != keep_only.end(); ++i)
        {
            int pos = posForCode(*i);
            if (pos == -1) continue;
            md1[pos] = md[pos];
        }

        if (!root)
            root = new RootNode(md1, stats);
        else
            root->add(md1, stats);
        return true;
    }
};
}

void Summary::add(const Summary& s)
{
    if (s.root)
    {
        summary::SummaryMerger merger(root);
        s.root->visit(merger);
    }
}

void Summary::add(const Summary& s, const std::set<types::Code>& keep_only)
{
    if (s.root)
    {
        summary::PruningSummaryMerger merger(keep_only, root);
        s.root->visit(merger);
    }
}

namespace summary {
struct MatchVisitor : public Visitor
{
    bool res;
    MatchVisitor() : res(false) {}
    virtual bool operator()(const std::vector< UItem<> >& md, const UItem<Stats>& stats)
    {
        res = true;
        // Stop iteration
        return false;
    }
};
}
bool Summary::match(const Matcher& matcher) const
{
    if (!root && matcher.m_impl) return false;

    summary::MatchVisitor visitor;
    root->visitFiltered(matcher, visitor);
    return visitor.res;
}

void Summary::filter(const Matcher& matcher, Summary& result) const
{
    if (root)
    {
        summary::SummaryMerger merger(result.root);
        root->visitFiltered(matcher, merger);
    }
}

Summary Summary::filter(const Matcher& matcher) const
{
    Summary res;
    filter(matcher, res);
    return res;
}

std::ostream& operator<<(std::ostream& o, const Summary& s)
{
    s.writeYaml(o);
    return o;
}


}
// vim:set ts=4 sw=4:
