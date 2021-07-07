#ifndef ARKI_STREAM_CONCRETE_TIMEOUT_H
#define ARKI_STREAM_CONCRETE_TIMEOUT_H

#include <arki/stream/concrete.h>
#include <arki/core/fwd.h>
#include <poll.h>

namespace arki {
namespace stream {

template<typename Backend>
class ConcreteTimeoutStreamOutputBase: public ConcreteStreamOutputBase<Backend>
{
public:
    using ConcreteStreamOutputBase<Backend>::ConcreteStreamOutputBase;
};

class ConcreteTimeoutStreamOutput: public ConcreteTimeoutStreamOutputBase<ConcreteLinuxBackend>
{
    using ConcreteTimeoutStreamOutputBase::ConcreteTimeoutStreamOutputBase;
};

}
}

#endif
