#include "inbound.h"
#include "arki/metadata.h"

namespace arki::metadata {

std::ostream& operator<<(std::ostream& o, Inbound::Result res)
{
    switch (res)
    {
        case Inbound::Result::OK: return o << "OK";
        case Inbound::Result::DUPLICATE: return o << "DUPLICATE";
        case Inbound::Result::ERROR: return o << "ERROR";
        default: return o << "<unknown>";
    }
}

Inbound::Inbound(std::shared_ptr<Metadata> md) : md(md) {}

Inbound::~Inbound() {}

void InboundBatch::set_all_error(const std::string& note)
{
    for (auto& e: *this)
    {
        e->destination.clear();
        e->md->add_note(note);
        e->result = Inbound::Result::ERROR;
    }
}


}

