#include "arki/runtime/arki-check.h"
#include "arki/runtime.h"
#include <iostream>

using namespace std;

int main(int argc, const char* argv[])
{
    try {
        arki::runtime::init();
        arki::runtime::ArkiCheck cmd;
        cmd.run(argc, argv);
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}
