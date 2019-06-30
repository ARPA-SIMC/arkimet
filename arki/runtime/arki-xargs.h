#ifndef ARKI_RUNTIME_ARKIXARGS_H
#define ARKI_RUNTIME_ARKIXARGS_H

namespace arki {
namespace runtime {

/// Runs a command on every blob of data found in the input files
class ArkiXargs
{
public:
    int run(int argc, const char* argv[]);
};

}
}

#endif
