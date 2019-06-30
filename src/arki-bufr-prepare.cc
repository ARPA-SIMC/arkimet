#include <arki/runtime/arki-bufr-prepare.h>

int main(int argc, const char* argv[])
{
    arki::runtime::ArkiBUFRPrepare cmd;
    return cmd.run(argc, argv);
}

