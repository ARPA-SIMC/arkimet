#ifndef ARKI_RUNTIME_ARKIBUFRPREPARE_H
#define ARKI_RUNTIME_ARKIBUFRPREPARE_H

namespace arki {
namespace runtime {

/// Prepare BUFR messages for importing into arkimet
class ArkiBUFRPrepare
{
public:
    int run(int argc, const char* argv[]);
};

}
}

#endif
