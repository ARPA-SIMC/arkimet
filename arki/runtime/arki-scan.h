#ifndef ARKI_RUNTIME_ARKISCAN_H
#define ARKI_RUNTIME_ARKISCAN_H

namespace arki {
namespace runtime {

int arki_scan(int argc, const char* argv[]);

class ArkiScan
{
public:
    int run(int argc, const char* argv[]);
};

}
}

#endif

