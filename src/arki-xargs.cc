#include <arki/runtime/arki-xargs.h>

int main(int argc, const char* argv[])
{
    arki::runtime::ArkiXargs cmd;
    return cmd.run(argc, argv);
}
