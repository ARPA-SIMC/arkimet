#include "short.h"
#include "arki/structured/emitter.h"
#include "arki/formatter.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace summary {

void Short::serialise(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.start_mapping();

    e.add("items");
    e.start_mapping();
    e.add("summarystats");
    e.start_mapping();
    stats.serialiseLocal(e, f);
    e.end_mapping();
    for (const auto& i: items)
    {
        e.add(str::lower(types::formatCode(i.first)));
        e.start_list();
        for (const auto& mi: i.second)
            e.add_type(*mi, keys, f);
        e.end_list();
    }
    e.end_mapping();
    e.end_mapping();
}

void Short::write_yaml(std::ostream& out, const Formatter* f) const
{
    out << "SummaryStats:" << endl;
    out << "  " << "Size: " << stats.size << endl;
    out << "  " << "Count: " << stats.count << endl;
    out << "  " << "Reftime: " << stats.begin << " to " << stats.end << endl;
    out << "Items:" << endl;
    for (const auto& i: items)
    {
        string uc = str::lower(types::formatCode(i.first));
        uc[0] = toupper(uc[0]);
        out << "  " << uc << ":" << endl;
        for (const auto& mi: i.second) {
            out << "    " << *mi;
            if (f)
                out << "\t# " << f->format(*mi);
            out << endl;
        }
    }
}

bool Short::operator()(const std::vector<const types::Type*>& md, const summary::Stats& stats)
{
    for (size_t i = 0; i < md.size(); ++i)
    {
        if (!md[i]) continue;
        types::Code code = codeForPos(i);
        items[code].insert(*md[i]);
    }
    // TODO: with public access to summary->root.stats() the merge could be skipped
    this->stats.merge(stats);
    return true;
}

}
}
