#include "summary.h"
#include "core/file.h"
#include "core/time.h"
#include "exceptions.h"
#include "formatter.h"
#include "iotrace.h"
#include "matcher.h"
#include "stream.h"
#include "structured/emitter.h"
#include "structured/keys.h"
#include "structured/reader.h"
#include "summary/codec.h"
#include "summary/stats.h"
#include "summary/table.h"
#include "types/area.h"
#include "types/bundle.h"
#include "utils/compress.h"
#include "utils/geos.h"
#include "utils/string.h"
#include "utils/sys.h"
#include <fcntl.h>

using namespace std;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::types;
using namespace arki::summary;

namespace arki {

Summary::Summary() : root(new summary::Table) {}

Summary::~Summary() { delete root; }

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
    arki::utils::geos::GeometryVector geoms;
    std::set<std::vector<uint8_t>> seen;

    bool operator()(const Type& type) override
    {
        const Area& a = *dynamic_cast<const Area*>(&type);
        vector<uint8_t> encoded;
        core::BinaryEncoder enc(encoded);
        a.encodeBinary(enc);
        pair<set<vector<uint8_t>>::iterator, bool> i = seen.insert(encoded);
        if (i.second)
        {
            const arki::utils::geos::Geometry& g = a.bbox();
            if (!g)
                return true;
            geoms.emplace_back(g.clone());
        }
        return true;
    }

    arki::utils::geos::Geometry makeBBox()
    {
        if (geoms.empty())
            return arki::utils::geos::Geometry();

        auto collection =
            arki::utils::geos::Geometry::create_collection(std::move(geoms));
        return collection.convex_hull();
    }
};
#endif
} // namespace summary

bool Summary::empty() const { return root->empty(); }

size_t Summary::count() const { return root->stats.count; }

unsigned long long Summary::size() const { return root->stats.size; }

void Summary::dump(std::ostream& out) const { root->dump(out); }

core::Interval Summary::get_reference_time() const
{
    if (root->empty())
        throw_consistency_error("get summary reference time",
                                "summary is empty");
    else
        return root->stats.make_interval();
}

void Summary::expand_date_range(core::Interval& interval) const
{
    if (root->empty())
        return;

    interval.extend(Interval(root->stats.begin, root->stats.end));
}

arki::utils::geos::Geometry Summary::getConvexHull() const
{
#ifdef HAVE_GEOS
    summary::StatsHull merger;
    root->visitItem(summary::Visitor::posForCode(TYPE_AREA), merger);
    return merger.makeBBox();
#else
    return arki::utils::geos::Geometry();
#endif
}

bool Summary::read(core::NamedFileDescriptor& in)
{
    iotrace::trace_file(in, 0, 0, "read summary");

    types::Bundle bundle;
    if (!bundle.read_header(in))
        return false;

    // Ensure first 2 bytes are SU
    if (bundle.signature != "SU")
        throw_consistency_error("parsing file " + in.path().native(),
                                "summary entry does not start with 'SU'");

    if (!bundle.read_data(in))
        return false;

    core::BinaryDecoder dec(bundle.data);
    read_inner(dec, bundle.version, in.path());

    return true;
}

bool Summary::read(core::AbstractInputFile& in)
{
    iotrace::trace_file(in, 0, 0, "read summary");

    types::Bundle bundle;
    if (!bundle.read_header(in))
        return false;

    // Ensure first 2 bytes are SU
    if (bundle.signature != "SU")
        throw_consistency_error("parsing file " + in.path().native(),
                                "summary entry does not start with 'SU'");

    if (!bundle.read_data(in))
        return false;

    core::BinaryDecoder dec(bundle.data);
    read_inner(dec, bundle.version, in.path());

    return true;
}

bool Summary::read(core::BinaryDecoder& dec,
                   const std::filesystem::path& filename)
{
    string signature;
    unsigned version;
    core::BinaryDecoder inner = dec.pop_metadata_bundle(signature, version);

    // Ensure first 2 bytes are SU
    if (signature != "SU")
        throw std::runtime_error("cannot parse file "s + filename.native() +
                                 ": summary entry does not start with 'SU'");

    read_inner(inner, version, filename);

    return true;
}

void Summary::read_inner(core::BinaryDecoder& dec, unsigned version,
                         const std::filesystem::path& filename)
{
    using namespace summary;
    summary::decode(dec, version, filename, *root);
}

std::vector<uint8_t> Summary::encode(bool compressed) const
{
    // Encode
    vector<uint8_t> inner;
    core::BinaryEncoder innerenc(inner);
    if (!root->empty())
    {
        EncodingVisitor visitor(innerenc);
        visit(visitor);
    }

    // Prepend header
    vector<uint8_t> res;
    core::BinaryEncoder enc(res);
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
        }
        else
        {
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
    }
    else
    {
        enc.add_unsigned(inner.size() + 1, 4);
        enc.add_unsigned(0u, 1);
        enc.add_raw(inner);
    }
    return res;
}

void Summary::write(NamedFileDescriptor& out) const
{
    // Prepare the encoded data
    std::vector<uint8_t> encoded = encode(true);

    iotrace::trace_file(out, 0, encoded.size(), "write summary");

    // Write out
    out.write(encoded.data(), encoded.size());
}

stream::SendResult Summary::write(StreamOutput& out) const
{
    // Prepare the encoded data
    std::vector<uint8_t> encoded = encode(true);

    iotrace::trace_file(out, 0, encoded.size(), "write summary");

    // Write out
    return out.send_buffer(encoded.data(), encoded.size());
}

void Summary::writeAtomically(const std::filesystem::path& fname)
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

    YamlPrinter(ostream& out, size_t indent, const Formatter* f = 0)
        : out(out), indent(indent, ' '), f(f)
    {
    }
    bool operator()(const std::vector<const Type*>& md,
                    const Stats& stats) override
    {
        // Write the metadata items
        out << "SummaryItem:" << endl;
        for (vector<const Type*>::const_iterator i = md.begin(); i != md.end();
             ++i)
        {
            if (!*i)
                continue;
            string ucfirst(str::lower((*i)->tag()));
            ucfirst[0] = toupper(ucfirst[0]);
            out << indent << ucfirst << ": ";
            (*i)->writeToOstream(out);
            if (f)
                out << "\t# " << f->format(**i);
            out << endl;
        }

        // Write the stats
        out << "SummaryStats:" << endl;
        out << indent << "Count: " << stats.count << endl;
        out << indent << "Size: " << stats.size << endl;
        out << indent << "Reftime: " << stats.begin << " to " << stats.end
            << endl;
        return true;
    }
};
} // namespace summary

bool Summary::visit(summary::Visitor& visitor) const
{
    if (root->empty())
        return true;
    return root->visit(visitor);
}

bool Summary::visitFiltered(const Matcher& matcher,
                            summary::Visitor& visitor) const
{
    if (root->empty())
        return true;
    if (matcher.empty())
        return root->visit(visitor);
    else
        return root->visitFiltered(matcher, visitor);
}

std::string Summary::to_yaml(const Formatter* formatter) const
{
    std::stringstream buf;
    if (root->empty())
        return buf.str();
    summary::YamlPrinter printer(buf, 2, formatter);
    visit(printer);
    return buf.str();
}

void Summary::write_yaml(core::NamedFileDescriptor& out,
                         const Formatter* formatter) const
{
    std::string yaml = to_yaml(formatter);
    out.write_all_or_retry(yaml.data(), yaml.size());
}

void Summary::serialise(structured::Emitter& e, const structured::Keys& keys,
                        const Formatter* f) const
{
    e.start_mapping();
    e.add(keys.summary_items);
    e.start_list();
    if (!root->empty())
    {
        struct Serialiser : public summary::Visitor
        {
            structured::Emitter& e;
            const structured::Keys& keys;
            const Formatter* f;

            Serialiser(structured::Emitter& e, const structured::Keys& keys,
                       const Formatter* f)
                : e(e), keys(keys), f(f)
            {
            }

            bool operator()(const std::vector<const Type*>& md,
                            const Stats& stats) override
            {
                e.start_mapping();
                for (std::vector<const Type*>::const_iterator i = md.begin();
                     i != md.end(); ++i)
                {
                    if (!*i)
                        continue;
                    e.add((*i)->tag());
                    e.start_mapping();
                    if (f)
                        e.add(keys.summary_desc, f->format(**i));
                    (*i)->serialise_local(e, keys, f);
                    e.end_mapping();
                }
                e.add(keys.summary_stats);
                e.start_mapping();
                stats.serialiseLocal(e, f);
                e.end_mapping();
                e.end_mapping();
                return true;
            }
        } visitor(e, keys, f);

        visit(visitor);
    }
    e.end_list();
    e.end_mapping();
}

void Summary::read(const structured::Keys& keys, const structured::Reader& val)
{
    using namespace structured::memory;

    val.sub(keys.summary_items, "summary item list",
            [&](const structured::Reader& items) {
                unsigned size = items.list_size("summary item list");
                for (unsigned i = 0; i < size; ++i)
                {
                    items.sub(i, "summary item",
                              [&](const structured::Reader& item) {
                                  root->merge(keys, item);
                              });
                }
            });
}

void Summary::read_file(const std::filesystem::path& fname)
{
    // Read all the metadata
    sys::File in(fname, O_RDONLY);
    read(in);
    in.close();
}

bool Summary::readYaml(LineReader& in, const std::filesystem::path& filename)
{
    return root->merge_yaml(in, filename);
}

void Summary::add(const Metadata& md) { return root->merge(md); }

void Summary::add(const Metadata& md, const summary::Stats& s)
{
    return root->merge(md, s);
}

namespace summary {
struct SummaryMerger : public Visitor
{
    Table& root;

    SummaryMerger(Table& root) : root(root) {}
    bool operator()(const std::vector<const Type*>& md,
                    const Stats& stats) override
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
        : positions(positions), root(root)
    {
    }

    bool operator()(const std::vector<const Type*>& md,
                    const Stats& stats) override
    {
        root.merge(md, stats, positions);
        return true;
    }
};
} // namespace summary

void Summary::add(const Summary& s)
{
    if (s.root->empty())
        return;
    summary::SummaryMerger merger(*root);
    s.visit(merger);
}

void Summary::add(const Summary& s, const std::set<types::Code>& keep_only)
{
    if (s.root->empty())
        return;
    vector<unsigned> positions;
    for (set<types::Code>::const_iterator i = keep_only.begin();
         i != keep_only.end(); ++i)
    {
        int pos = Visitor::posForCode(*i);
        if (pos < 0)
            continue;
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
    bool operator()(const std::vector<const Type*>& md,
                    const Stats& stats) override
    {
        res = true;
        // Stop iteration
        return false;
    }
};
} // namespace summary
bool Summary::match(const Matcher& matcher) const
{
    if (root->empty() && !matcher.empty())
        return false;

    summary::MatchVisitor visitor;
    visitFiltered(matcher, visitor);
    return visitor.res;
}

void Summary::filter(const Matcher& matcher, Summary& result) const
{
    if (root->empty())
        return;
    summary::SummaryMerger merger(*result.root);
    visitFiltered(matcher, merger);
}

} // namespace arki
