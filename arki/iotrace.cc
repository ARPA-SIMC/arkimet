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

#include "config.h"

#include <arki/iotrace.h>
#include <arki/utils/intern.h>
#include <arki/runtime/config.h>
#include <vector>
#include <set>
#include <fstream>
#include <stdint.h>

using namespace std;

namespace arki {
namespace iotrace {

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

static utils::StringInternTable* itable = 0;
static ListenerList* listeners = 0;

void init()
{
    // Prevent double initialisation
    if (itable) return;

    itable = new utils::StringInternTable;

    if (!runtime::Config::get().file_iotrace_output.empty())
    {
        ostream* out = new ofstream(runtime::Config::get().file_iotrace_output.c_str());
        Logger* logger = new Logger(*out);
        add_listener(*logger);
        // Lose references, effectively creating garbage; never mind, as we log
        // until the end of the program.
    }
}

void trace_file(const std::string& name, off64_t offset, size_t size, const char* desc)
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

void trace_file(const char* name, off64_t offset, size_t size, const char* desc)
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

void Collector::dump(std::ostream& out) const
{
    for (vector<Event>::const_iterator i = events.begin();
            i != events.end(); ++i)
        out << i->filename() << ":" << i->offset << ":" << i->size << ": " << i->desc << endl;
}

void Logger::operator()(const Event& e)
{
    out << e.filename() << ":" << e.offset << ":" << e.size << ": " << e.desc << endl;
}

}
}
