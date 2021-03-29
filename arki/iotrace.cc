#include "arki/iotrace.h"
#include "arki/exceptions.h"
#include "arki/core/file.h"
#include "arki/stream.h"
#include "arki/runtime.h"
#include <vector>
#include <ostream>

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

static ListenerList* listeners = 0;

void init()
{
    if (!Config::get().file_iotrace_output.empty())
    {
        FILE* out = fopen(Config::get().file_iotrace_output.c_str(), "at");
        if (!out) throw_system_error("cannot open " + Config::get().file_iotrace_output + " for appending");
        Logger* logger = new Logger(out);
        add_listener(*logger);
        // Lose references, effectively creating garbage; never mind, as we log
        // until the end of the program.
    }
}

void trace_file(const std::string& name, off_t offset, size_t size, const char* desc)
{
    if (listeners)
    {
        Event ev;
        ev.filename = name;
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
        ev.filename = name;
        ev.offset = offset;
        ev.size = size;
        ev.desc = desc;
        listeners->process(ev);
    }
}

void trace_file(core::NamedFileDescriptor& fd, off_t offset, size_t size, const char* desc)
{
    if (listeners)
    {
        Event ev;
        ev.filename = fd.name();
        ev.offset = offset;
        ev.size = size;
        ev.desc = desc;
        listeners->process(ev);
    }
}

void trace_file(core::AbstractInputFile& fd, off_t offset, size_t size, const char* desc)
{
    if (listeners)
    {
        Event ev;
        ev.filename = fd.name();
        ev.offset = offset;
        ev.size = size;
        ev.desc = desc;
        listeners->process(ev);
    }
}

void trace_file(core::AbstractOutputFile& fd, off_t offset, size_t size, const char* desc)
{
    if (listeners)
    {
        Event ev;
        ev.filename = fd.name();
        ev.offset = offset;
        ev.size = size;
        ev.desc = desc;
        listeners->process(ev);
    }
}

void trace_file(StreamOutput& out, off_t offset, size_t size, const char* desc)
{
    if (listeners)
    {
        Event ev;
        ev.filename = out.name();
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
        out << i->filename << ":" << i->offset << ":" << i->size << ": " << i->desc << endl;
}

void Logger::operator()(const Event& e)
{
    fprintf(out, "%s:%zu:%zu:%s\n", e.filename.c_str(), (size_t)e.offset, e.size, e.desc);
}

}
}
