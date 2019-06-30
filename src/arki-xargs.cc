/// Runs a command on every blob of data found in the input files
#include <arki/runtime/arki-xargs.h>

int main(int argc, const char* argv[])
{
    arki::runtime::ArkiXargs cmd;
    return cmd.run(argc, argv);
}
