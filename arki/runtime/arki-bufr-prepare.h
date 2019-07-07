#ifndef ARKI_RUNTIME_ARKIBUFRPREPARE_H
#define ARKI_RUNTIME_ARKIBUFRPREPARE_H

#include <string>
#include <vector>

namespace arki {
namespace runtime {

/// Prepare BUFR messages for importing into arkimet
class ArkiBUFRPrepare
{
public:
    bool force_usn = false;
    int forced_usn;
    std::string outfile;
    std::string failfile;
    std::vector<std::string> inputs;

    int run();
};

}
}

#endif
