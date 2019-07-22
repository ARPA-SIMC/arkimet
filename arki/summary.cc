#include "summary.h"
#include "summary/table.h"
#include "summary/codec.h"
#include "summary/stats.h"
#include "exceptions.h"
#include "metadata.h"
#include "matcher.h"
#include "binary.h"
#include "formatter.h"
#include "core/time.h"
#include "types/utils.h"
#include "types/area.h"
#include "utils/geos.h"
#include "utils/compress.h"
#include "emitter.h"
#include "emitter/memory.h"
#include "iotrace.h"
#include "utils/lua.h"
#include "utils/string.h"
#include "utils/sys.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::types;
using namespace arki::summary;

namespace arki {

Summary::Summary()
    : root(new summary::Table)
{
}

Summary::~Summary()
{
    delete root;
}

#ifdef HAVE_LUA
namespace {

struct LuaPusher: public summary::Visitor
{
    lua_State* L;
    int index;

    LuaPusher(lua_State* L) : L(L), index(1) {}
    virtual bool operator()(const std::vector<const Type*>& md, const Stats& stats)
    {
        // Table with the couple
        lua_newtable(L);
        // Table with the items
        lua_newtable(L);
        // Push the items
        for (size_t i = 0; i < md.size(); ++i)
        {
            // Name
            if (md[i])
            {
                // Key
                string name = str::lower(types::formatCode(codeForPos(i)));
                lua_pushlstring(L, name.data(), name.size());
                // Value
                md[i]->lua_push(L);
                lua_rawset(L, -3);
            }
        }
        lua_rawseti(L, -2, 1);
        // Push the stats
        stats.lua_push(L);
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
        unique_ptr<Summary> new_summary(new Summary);
        s->filter(m, *new_summary);
        SummaryUD::create(L, new_summary.release(), true);
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
    ud->val->add(*s);

    // Set the summary for the userdata
    arkilua_getmetatable(L);
    lua_setmetatable(L, -2);

    return 1;
}

// Add a summary to this summary
static int arkilua_add_summary(lua_State* L)
{
    Summary* s = Summary::lua_check(L, 1);
    luaL_argcheck(L, s != NULL, 1, "`arki.summary' expected");
    Summary* s1 = Summary::lua_check(L, 2);
    luaL_argcheck(L, s != NULL, 2, "`arki.summary' expected");

    s->add(*s1);

    return 0;
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

static const struct luaL_Reg summaryclasslib [] = {
    { "new", arkilua_new },
    { NULL, NULL }
};

static const struct luaL_Reg summarylib [] = {
    { "count", arkilua_count },
    { "size", arkilua_size },
    { "data", arkilua_data },
    { "filter", arkilua_filter },
    { "copy", arkilua_copy },
    { "add_summary", arkilua_add_summary },
    { "__gc", arkilua_gc },
    { "__tostring", arkilua_tostring },
    { NULL, NULL }
};

static void arkilua_getmetatable(lua_State* L)
{
    // Set the metatable for the userdata
    if (luaL_newmetatable(L, "arki.summary"))
    {
        // If the metatable wasn't previously created, create it now
        lua_pushstring(L, "__index");
        lua_pushvalue(L, -2);  /* pushes the metatable */
        lua_settable(L, -3);  /* metatable.__index = metatable */

        // Load normal methods
        utils::lua::add_functions(L, summarylib);
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
    utils::lua::add_arki_global_library(L, "summary", summaryclasslib);
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
    return root->equals(*m.root);
}

void Summary::clear()
{
    delete root;
    root = new summary::Table;
}

namespace summary {
#ifdef HAVE_GEOS
struct StatsHull : public ItemVisitor
{
    const arki::utils::geos::GeometryFactory* gf;
    vector<arki::utils::geos::Geometry*>* geoms;
    std::set<std::vector<uint8_t>> seen;

    StatsHull()
        : gf(arki::utils::geos::GeometryFactory::getDefaultInstance()),
          geoms(new vector<arki::utils::geos::Geometry*>)
    {
    }

    virtual ~StatsHull()
    {
        if (geoms)
        {
            for (auto i: *geoms)
                delete i;
            delete geoms;
        }
    }

    bool operator()(const Type& type) override
    {
        const Area& a = *dynamic_cast<const Area*>(&type);
        vector<uint8_t> encoded;
        BinaryEncoder enc(encoded);
        a.encodeBinary(enc);
        pair<set<vector<uint8_t>>::iterator, bool> i = seen.insert(encoded);
        if (i.second)
        {
            const arki::utils::geos::Geometry* g = a.bbox();
            //cerr << "Got: " << g << g->getGeometryType() << endl;
            if (!g) return true;
            //cerr << "Adding: " << g->toString() << endl;
            geoms->push_back(g->clone());
        }
        return true;
    }

    unique_ptr<arki::utils::geos::Geometry> makeBBox()
    {
        if (geoms->empty())
            return unique_ptr<arki::utils::geos::Geometry>();

        auto gc(gf->createGeometryCollection(geoms));
        geoms = 0;
        return unique_ptr<arki::utils::geos::Geometry>(gc->convexHull());
    }
};
#endif
}

size_t Summary::count() const
{
    return root->stats.count;
}

unsigned long long Summary::size() const
{
    return root->stats.size;
}

void Summary::dump(std::ostream& out) const
{
    root->dump(out);
}

unique_ptr<types::Reftime> Summary::getReferenceTime() const
{
    if (root->empty())
        throw_consistency_error("get summary reference time", "summary is empty");
    else
        return root->stats.make_reftime();
}

void Summary::expand_date_range(unique_ptr<Time>& begin, unique_ptr<Time>& end) const
{
    if (root->empty())
        return;

    if (!begin.get())
        begin.reset(new Time(root->stats.begin));
    else if (*begin > root->stats.begin)
        *begin = root->stats.begin;

    if (!end.get())
        end.reset(new Time(root->stats.end));
    else if (*end < root->stats.end)
        *end = root->stats.end;
}

namespace {
struct ResolveVisitor : public summary::Visitor
{
    std::vector<types::ItemSet>& result;
    std::vector<types::Code> codes;
    size_t added;

    ResolveVisitor(std::vector<types::ItemSet>& result, const Matcher& m) : result(result), added(0)
    {
        m.foreach_type([&](types::Code code, const matcher::OR&) {
            codes.push_back(code);
        });
    }
    virtual ~ResolveVisitor() {}
    virtual bool operator()(const std::vector<const Type*>& md, const summary::Stats& stats)
    {
        types::ItemSet is;
        for (std::vector<types::Code>::const_iterator i = codes.begin();
                i != codes.end(); ++i)
        {
            int pos = posForCode(*i);
            if (!md[pos]) return true;
            is.set(*md[pos]);
        }
        ++added;
        // Insertion sort, as we expect to have lots of duplicates
        std::vector<types::ItemSet>::iterator i = std::lower_bound(result.begin(), result.end(), is);
        if (i == result.end())
            result.push_back(is);
        else if (*i != is)
            result.insert(i, is);

        return true;
    }
};
}

std::vector<types::ItemSet> Summary::resolveMatcher(const Matcher& matcher) const
{
    if (matcher.empty()) return std::vector<types::ItemSet>();

    std::vector<types::ItemSet> result;
    ResolveVisitor visitor(result, matcher);
    visitFiltered(matcher, visitor);

    return result;
}

size_t Summary::resolveMatcher(const Matcher& matcher, std::vector<types::ItemSet>& res) const
{
    if (matcher.empty()) return 0;

    ResolveVisitor visitor(res, matcher);
    visitFiltered(matcher, visitor);
    return visitor.added;
}

std::unique_ptr<arki::utils::geos::Geometry> Summary::getConvexHull() const
{
#ifdef HAVE_GEOS
    summary::StatsHull merger;
    root->visitItem(summary::Visitor::posForCode(TYPE_AREA), merger);
    return merger.makeBBox();
#else
    return std::unique_ptr<arki::utils::geos::Geometry>();
#endif
}


bool Summary::read(int fd, const std::string& filename)
{
    iotrace::trace_file(filename, 0, 0, "read summary");

    types::Bundle bundle;
    NamedFileDescriptor f(fd, filename);
    if (!bundle.read_header(f))
        return false;

    // Ensure first 2 bytes are SU
    if (bundle.signature != "SU")
        throw_consistency_error("parsing file " + filename, "summary entry does not start with 'SU'");

    if (!bundle.read_data(f))
        return false;

    BinaryDecoder dec(bundle.data);
    read_inner(dec, bundle.version, filename);

    return true;
}

bool Summary::read(BinaryDecoder& dec, const std::string& filename)
{
    string signature;
    unsigned version;
    BinaryDecoder inner = dec.pop_metadata_bundle(signature, version);

    // Ensure first 2 bytes are SU
    if (signature != "SU")
        throw std::runtime_error("cannot parse file " + filename + ": summary entry does not start with 'SU'");

    read_inner(inner, version, filename);

    return true;
}

void Summary::read_inner(BinaryDecoder& dec, unsigned version, const std::string& filename)
{
    using namespace summary;
    summary::decode(dec, version, filename, *root);
}

std::vector<uint8_t> Summary::encode(bool compressed) const
{
    // Encode
    vector<uint8_t> inner;
    BinaryEncoder innerenc(inner);
    if (!root->empty())
    {
        EncodingVisitor visitor(innerenc);
        visit(visitor);
    }

    // Prepend header
    vector<uint8_t> res;
    BinaryEncoder enc(res);
    // Signature
    enc.add_string("SU");
    // Version
    enc.add_unsigned(3u, 2);
    if (compressed)
    {
        vector<uint8_t> comp = utils::compress::lzo(inner.data(), inner.size());
        if (comp.size() + 4 >= inner.size())
        {
            // No point in compressing
            enc.add_unsigned(inner.size() + 1, 4);
            // Add compression type (uncompressed)
            enc.add_unsigned(0u, 1);
            enc.add_raw(inner);
        } else {
            // Compression makes sense
            // Add total size
            enc.add_unsigned(comp.size() + 5, 4);
            // Add compression type (LZO)
            enc.add_unsigned(1u, 1);
            // Add uncompressed size
            enc.add_unsigned(inner.size(), 4);
            // Add compressed data
            enc.add_raw(comp);
        }
    } else {
        enc.add_unsigned(inner.size() + 1, 4);
        enc.add_unsigned(0u, 1);
        enc.add_raw(inner);
    }
    return res;
}

void Summary::write(int outfd, const std::string& filename) const
{
    // Prepare the encoded data
    vector<uint8_t> encoded = encode(true);

    iotrace::trace_file(filename, 0, encoded.size(), "write summary");

    // Write out
    sys::NamedFileDescriptor out(outfd, filename);
    out.write(encoded.data(), encoded.size());
}

void Summary::writeAtomically(const std::string& fname)
{
    vector<uint8_t> enc = encode(true);
    iotrace::trace_file(fname, 0, enc.size(), "write summary");
    sys::write_file_atomically(fname, enc.data(), enc.size(), 0666);
}

namespace summary {
struct YamlPrinter : public Visitor
{
    ostream& out;
    string indent;
    const Formatter* f;

    YamlPrinter(ostream& out, size_t indent, const Formatter* f = 0) : out(out), indent(indent, ' '), f(f) {}
    virtual bool operator()(const std::vector<const Type*>& md, const Stats& stats)
    {
        // Write the metadata items
        out << "SummaryItem:" << endl;
        for (vector<const Type*>::const_iterator i = md.begin(); i != md.end(); ++i)
        {
            if (!*i) continue;
            string ucfirst(str::lower((*i)->tag()));
            ucfirst[0] = toupper(ucfirst[0]);
            out << indent << ucfirst << ": ";
            (*i)->writeToOstream(out);
            if (f) out << "\t# " << (*f)(**i);
            out << endl;
        }

        // Write the stats
        out << "SummaryStats:" << endl;
        unique_ptr<Reftime> reftime(stats.make_reftime());
        out << indent << "Count: " << stats.count << endl;
        out << indent << "Size: " << stats.size << endl;
        out << indent << "Reftime: " << *reftime << endl;
        return true;
    }
};
}

bool Summary::visit(summary::Visitor& visitor) const
{
    if (root->empty()) return true;
    return root->visit(visitor);
}

bool Summary::visitFiltered(const Matcher& matcher, summary::Visitor& visitor) const
{
    if (root->empty()) return true;
    if (matcher.empty())
        return root->visit(visitor);
    else
        return root->visitFiltered(matcher, visitor);
}

void Summary::write_yaml(std::ostream& out, const Formatter* f) const
{
    if (root->empty()) return;
    summary::YamlPrinter printer(out, 2, f);
    visit(printer);
}

void Summary::serialise(Emitter& e, const Formatter* f) const
{
    e.start_mapping();
    e.add("items");
    e.start_list();
    if (!root->empty())
    {
        struct Serialiser : public summary::Visitor
        {
            Emitter& e;
            const Formatter* f;

            Serialiser(Emitter& e, const Formatter* f) : e(e), f(f) {}

            virtual bool operator()(const std::vector<const Type*>& md, const Stats& stats)
            {
                e.start_mapping();
                for (std::vector<const Type*>::const_iterator i = md.begin();
                        i != md.end(); ++i)
                {
                    if (!*i) continue;
                    e.add((*i)->tag());
                    e.start_mapping();
                    if (f) e.add("desc", (*f)(**i));
                    (*i)->serialiseLocal(e, f);
                    e.end_mapping();
                }
                e.add("summarystats");
                e.start_mapping();
                stats.serialiseLocal(e, f);
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
        root->merge(m);
    }
}

void Summary::readFile(const std::string& fname)
{
    // Read all the metadata
    sys::File in(fname, O_RDONLY);
    read(in, fname);
    in.close();
}

bool Summary::readYaml(LineReader& in, const std::string& filename)
{
    return root->merge_yaml(in, filename);
}


void Summary::add(const Metadata& md)
{
    return root->merge(md);
}

void Summary::add(const Metadata& md, const summary::Stats& s)
{
    return root->merge(md, s);
}

namespace summary {
struct SummaryMerger : public Visitor
{
    Table& root;

    SummaryMerger(Table& root) : root(root) {}
    virtual bool operator()(const std::vector<const Type*>& md, const Stats& stats)
    {
        root.merge(md, stats);
        return true;
    }
};
struct PruningSummaryMerger : public Visitor
{
    const vector<unsigned> positions;
    Table& root;

    PruningSummaryMerger(const vector<unsigned>& positions, Table& root)
        : positions(positions), root(root) {}

    virtual bool operator()(const std::vector<const Type*>& md, const Stats& stats)
    {
        root.merge(md, stats, positions);
        return true;
    }
};
}

void Summary::add(const Summary& s)
{
    if (s.root->empty()) return;
    summary::SummaryMerger merger(*root);
    s.visit(merger);
}

void Summary::add(const Summary& s, const std::set<types::Code>& keep_only)
{
    if (s.root->empty()) return;
    vector<unsigned> positions;
    for (set<types::Code>::const_iterator i = keep_only.begin();
            i != keep_only.end(); ++i)
    {
        int pos = Visitor::posForCode(*i);
        if (pos < 0) continue;
        positions.push_back(pos);
    }
    summary::PruningSummaryMerger merger(positions, *root);
    s.visit(merger);
}

namespace summary {
struct MatchVisitor : public Visitor
{
    bool res;
    MatchVisitor() : res(false) {}
    virtual bool operator()(const std::vector<const Type*>& md, const Stats& stats)
    {
        res = true;
        // Stop iteration
        return false;
    }
};
}
bool Summary::match(const Matcher& matcher) const
{
    if (root->empty() && !matcher.empty()) return false;

    summary::MatchVisitor visitor;
    visitFiltered(matcher, visitor);
    return visitor.res;
}

void Summary::filter(const Matcher& matcher, Summary& result) const
{
    if (root->empty()) return;
    summary::SummaryMerger merger(*result.root);
    visitFiltered(matcher, merger);
}

std::ostream& operator<<(std::ostream& o, const Summary& s)
{
    s.write_yaml(o);
    return o;
}

}
