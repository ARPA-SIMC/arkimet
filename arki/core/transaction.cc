#include "transaction.h"

using namespace arki;

namespace arki {
namespace core {

Transaction::~Transaction() {}

Pending::Pending(Transaction* trans) : trans(trans)
{
}

Pending::Pending(Pending&& p) : trans(p.trans)
{
    p.trans = nullptr;
}

Pending::~Pending()
{
    if (trans)
    {
        trans->rollback_nothrow();
        delete trans;
    }
}

Pending& Pending::operator=(Pending&& p)
{
    // Prevent damage on assignment to self
    if (&p == this) return *this;

    // There should not be two pendings with the same transaction, but handle
    // it just in case
    if (trans && p.trans != trans)
    {
        trans->rollback();
        delete trans;
    }

    trans = p.trans;
    p.trans = nullptr;

    return *this;
}

void Pending::commit()
{
    if (trans)
    {
        trans->commit();
        delete trans;
        trans = 0;
    }
}

void Pending::rollback()
{
    if (trans)
    {
        trans->rollback();
        delete trans;
        trans = 0;
    }
}

void Pending::rollback_nothrow() noexcept
{
    if (trans)
    {
        trans->rollback_nothrow();
        delete trans;
        trans = 0;
    }
}

}
}
