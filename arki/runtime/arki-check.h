#ifndef ARKI_RUNTIME_ARKICHECK_H
#define ARKI_RUNTIME_ARKICHECK_H

namespace arki {
namespace runtime {

int arki_check(int argc, const char* argv[]);

class ArkiCheck
{
public:
    int run(int argc, const char* argv[]);
};

}
}

#endif

