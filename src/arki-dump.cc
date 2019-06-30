/// Dump arkimet data files
#include <arki/runtime/arki-dump.h>

int main(int argc, const char* argv[])
{
    arki::runtime::ArkiDump cmd;
    return cmd.run(argc, argv);
}
