#include "intern.h"
#include <vector>
#include <set>
#include <cstdint>

using namespace std;

namespace arki {
namespace utils {

static const unsigned TABLE_SIZE = 1024;

StringInternTable::StringInternTable(size_t table_size)
    : table_size(table_size), table(new Bucket*[table_size])
{
    for (unsigned i = 0; i < TABLE_SIZE; ++i)
        table[i] = 0;
}

StringInternTable::~StringInternTable()
{
    for (unsigned i = 0; i < TABLE_SIZE; ++i)
        if (table[i])
            delete table[i];
    delete[] table;
}

// Bernstein hash function
unsigned StringInternTable::hash(const char *str)
{
    if (str == NULL) return 0;

    uint32_t h = 5381;

    for (const char *s = str; *s; ++s)
        h = (h * 33) ^ *s;

    return h % table_size;
}

// Bernstein hash function
unsigned StringInternTable::hash(const std::string& str)
{
    uint32_t h = 5381;
    for (string::const_iterator s = str.begin(); s != str.end(); ++s)
        h = (h * 33) ^ *s;

    return h % table_size;
}

}
}
