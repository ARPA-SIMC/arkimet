#include "arki/runtime/arki-scan.h"
#include "arki/runtime.h"
#include <iostream>

using namespace std;

int main(int argc, const char* argv[])
{
    try {
        arki::runtime::init();
        return arki::runtime::arki_scan(argc, argv);
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}
