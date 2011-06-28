/*
 * iotrace - I/O profiling
 *
 * Copyright (C) 2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */
#include <arki/iotrace.h>
#include <arki/runtime/config.h>
#include <vector>
#include <set>
#include <stdint.h>

using namespace std;

namespace arki {
namespace iotrace {

static const unsigned TABLE_SIZE = 1024;

namespace {

// Bernstein hash function
uint32_t hash(const char *str)
{
    if (str == NULL) return 0;

    uint32_t h = 5381;

    for (const char *s = str; *s; ++s)
        h = (h * 33) ^ *s;

    return h % TABLE_SIZE;
}

// Bernstein hash function
uint32_t hash(const std::string& str)
{
    uint32_t h = 5381;
    for (string::const_iterator s = str.begin(); s != str.end(); ++s)
        h = (h * 33) ^ *s;

    return h % TABLE_SIZE;
}

}

/**
 * Hashtable-based string interning
 */
struct StringInternTable
{
    struct Bucket
    {
        std::string val;
        int id;
        Bucket* next;

        Bucket() : id(-1), next(0) {}
        Bucket(const std::string& val) : val(val), id(-1), next(0) {}
        ~Bucket()
        {
            if (next) delete next;
        }

        template<typename T>
        Bucket* get(const T& val)
        {
            if (this->val == val)
                return this;
            if (next)
                return next->get(val);
            return next = new Bucket(val);
        }
    };

    // Hash table
    Bucket* table[TABLE_SIZE];
    // Interned string table
    vector<string> string_table;


    StringInternTable()
    {
        for (unsigned i = 0; i < TABLE_SIZE; ++i)
            table[i] = 0;
    }
    ~StringInternTable()
    {
        for (unsigned i = 0; i < TABLE_SIZE; ++i)
            if (table[i])
                delete table[i];
    }

    // Get the intern ID for a string
    template<typename T>
    unsigned intern(const T& val)
    {
        uint32_t pos = hash(val);
        Bucket* res = 0;
        if (table[pos])
            res = table[pos]->get(val);
        else
            res = table[pos] = new Bucket(val);
        if (res->id == -1)
        {
            res->id = string_table.size();
            // In case strings can share their memory representation, share the
            // reference to the hashtable buckets here
            string_table.push_back(res->val);
        }
        return res->id;
    }
};

struct ListenerList
{
    Listener* l;
    ListenerList* next;
    ListenerList(Listener* l) : l(l), next(0) {}
    ListenerList(Listener* l, ListenerList* next) : l(l), next(next) {}

    void process(const Event& e)
    {
        (*l)(e);
        if (next) next->process(e);
    }

    // Remove a listener from the list
    void remove(Listener* l)
    {
        if (!next) return;
        if (next->l == l)
        {
            ListenerList* old = next;
            next = next->next;
            delete old;
        } else
            next->remove(l);
    }
};

static StringInternTable* itable = 0;
static ListenerList* listeners = 0;

void init()
{
    if (!itable) itable = new StringInternTable;

    if (!runtime::Config::get().file_iotrace_output.empty())
    {
        runtime::Config::get().file_iotrace_output;
    }
}

void trace_file(const std::string& name, off_t offset, size_t size, const char* desc)
{
    if (listeners)
    {
        Event ev;
        ev.file_id = itable->intern(name);
        ev.offset = offset;
        ev.size = size;
        ev.desc = desc;
        listeners->process(ev);
    }
}

void trace_file(const char* name, off_t offset, size_t size, const char* desc)
{
    if (listeners)
    {
        Event ev;
        ev.file_id = itable->intern(name);
        ev.offset = offset;
        ev.size = size;
        ev.desc = desc;
        listeners->process(ev);
    }
}

void add_listener(Listener& l)
{
    listeners = new ListenerList(&l, listeners);
}

void remove_listener(Listener& l)
{
    if (!listeners) return;
    if (listeners->l == &l)
    {
        ListenerList* old = listeners;
        listeners = listeners->next;
        delete old;
    } else
        listeners->remove(&l);
}

const std::string& Event::filename() const
{
    return itable->string_table[file_id];
}


Collector::Collector()
{
    add_listener(*this);
}

Collector::~Collector()
{
    remove_listener(*this);
}

void Collector::operator()(const Event& e)
{
    events.push_back(e);
}

}
}
