#include <iostream>
#include "parser.h"

using namespace std;
using namespace arki::matcher::reftime;

int main(int argc, char* argv[])
{
	Parser p;
	cout << "RES " << p.parse(argv[1]) << endl;
	for (size_t i = 0; i < p.res.size(); ++i)
		cout << " -> " << p.res[i]->toString() << endl;
	return 0;
}
