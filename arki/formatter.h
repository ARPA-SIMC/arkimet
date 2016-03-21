#ifndef ARKI_FORMATTER_H
#define ARKI_FORMATTER_H

/// Prettyprinter for arkimet types

#include <arki/types.h>
#include <string>
#include <memory>

namespace arki {

class Formatter
{
protected:
    Formatter();

public:
    virtual ~Formatter();
    virtual std::string operator()(const types::Type& v) const;

    static std::unique_ptr<Formatter> create();
};


}
#endif
