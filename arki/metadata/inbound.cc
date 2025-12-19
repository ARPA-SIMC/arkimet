#include "inbound.h"
#include "arki/metadata.h"

namespace arki::metadata {

/*
 * Inbound
 */

std::ostream& operator<<(std::ostream& o, Inbound::Result res)
{
    switch (res)
    {
        case Inbound::Result::OK:        return o << "OK";
        case Inbound::Result::DUPLICATE: return o << "DUPLICATE";
        case Inbound::Result::ERROR:     return o << "ERROR";
        default:                         return o << "<unknown>";
    }
}

Inbound::Inbound(std::shared_ptr<Metadata> md) : md(md) {}

Inbound::~Inbound() {}

/*
 * InboundBatch
 */

void InboundBatch::add(std::shared_ptr<Metadata> md)
{
    emplace_back(std::make_shared<Inbound>(md));
}

void InboundBatch::set_all_error(const std::string& note)
{
    for (auto& e : *this)
    {
        e->destination.clear();
        e->md->add_note(note);
        e->result = Inbound::Result::ERROR;
    }
}

} // namespace arki::metadata
