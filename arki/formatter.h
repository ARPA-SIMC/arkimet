#ifndef ARKI_FORMATTER_H
#define ARKI_FORMATTER_H

/// Prettyprinter for arkimet types

#include <arki/types/fwd.h>
#include <string>
#include <memory>

namespace arki {

class Formatter
{
protected:
    Formatter();

public:
    virtual ~Formatter();
    virtual std::string format(const types::Type& v) const;

    static std::unique_ptr<Formatter> create();
    static void set_factory(std::function<std::unique_ptr<Formatter>()> new_factory);
};


}
#endif
