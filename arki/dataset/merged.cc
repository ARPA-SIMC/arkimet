/*
 * dataset/merged - Access many datasets at the same time
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/merged.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>

#include <wibble/string.h>
#include <wibble/sys/mutex.h>
#include <wibble/sys/thread.h>

// #include <iostream>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {

/// Fixed size synchronised queue
template<typename T>
class SyncBuffer
{
protected:
	sys::Mutex mutex;
	sys::Condition cond;
	T* buffer;
	size_t head, tail, size;
	mutable bool m_done;

public:
	SyncBuffer(size_t size = 10) : head(0), tail(0), size(size), m_done(false)
	{
		buffer = new T[size];
	}
	~SyncBuffer()
	{
		delete[] buffer;
	}

	void push(const T& val)
	{
		sys::MutexLock lock(mutex);
		while ((head + 1) % size == tail)
			cond.wait(lock);
		buffer[head] = val;
		head = (head + 1) % size;
		cond.broadcast();
	}

	void pop()
	{
		sys::MutexLock lock(mutex);
		if (head == tail)
			throw wibble::exception::Consistency("removing an element from a SyncBuffer", "the buffer is empty");
		// Reset the item (this will take care, for example, to dereference
		// refcounted values in the same thread that took them over)
		buffer[tail] = T();
		tail = (tail + 1) % size;
		cond.broadcast();
	}

	// Get must be done by the same thread that does pop
	T* get()
	{
		sys::MutexLock lock(mutex);
		while (head == tail && !m_done)
			cond.wait(lock);
		if (head == tail && m_done) return 0;
		return &buffer[tail];
	}

	bool isDone()
	{
		sys::MutexLock lock(mutex);
		if (head != tail)
			return false;
		return m_done;
	}

	void done()
	{
		sys::MutexLock lock(mutex);
		m_done = true;
		cond.broadcast();
	}
};

class MetadataReader : public sys::Thread
{
protected:
	struct Consumer : public MetadataConsumer
	{
		SyncBuffer<Metadata>& buf;

		Consumer(SyncBuffer<Metadata>& buf) : buf(buf) {}

		virtual bool operator()(Metadata& m)
		{
			buf.push(m);
			return true;
		}
	};
	ReadonlyDataset* dataset;
	const Matcher* matcher;
	bool withData;
	string errorbuf;

	virtual void* main()
	{
		try {
			Consumer cons(mdbuf);
			dataset->queryMetadata(*matcher, withData, cons);
			mdbuf.done();
			return 0;
		} catch (std::exception& e) {
			mdbuf.done();
			errorbuf = e.what();
			return (void*)errorbuf.c_str();
		}
	}

public:
	SyncBuffer<Metadata> mdbuf;

	MetadataReader() : dataset(0), matcher(0), withData(false) {}

	void init(ReadonlyDataset& dataset, const Matcher& matcher, bool withData)
	{
		this->dataset = &dataset;
		this->matcher = &matcher;
		this->withData = withData;
	}
};

class SummaryReader : public sys::Thread
{
protected:
	const Matcher* matcher;
	ReadonlyDataset* dataset;
	string errorbuf;

	virtual void* main()
	{
		try {
			dataset->querySummary(*matcher, summary);
			return 0;
		} catch (std::exception& e) {
			errorbuf = e.what();
			return (void*)errorbuf.c_str();
		}
	}

public:
	Summary summary;

	SummaryReader() : matcher(0), dataset(0) {}

	void init(const Matcher& matcher, ReadonlyDataset& dataset)
	{
		this->matcher = &matcher;
		this->dataset = &dataset;
	}
};

Merged::Merged()
{
}

Merged::~Merged()
{
}

void Merged::addDataset(ReadonlyDataset& ds)
{
	datasets.push_back(&ds);
}

void Merged::queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer)
{
	// Handle the trivial case of only one dataset
	if (datasets.size() == 1)
	{
		datasets[0]->queryMetadata(matcher, withData, consumer);
		return;
	}

	MetadataReader readers[datasets.size()];

	// Start all the readers
	for (size_t i = 0; i < datasets.size(); ++i)
	{
		readers[i].init(*datasets[i], matcher, withData);
		readers[i].start();
	}

	// Output items in time-sorted order
	while (true)
	{
		int minmd = -1;
		UItem<types::Reftime> mintime;

		for (size_t i = 0; i < datasets.size(); ++i)
		{
			Metadata* md = readers[i].mdbuf.get();
			if (!md) continue;

			UItem<types::Reftime> mdtime = md->get(types::TYPE_REFTIME).upcast<types::Reftime>();
			if (!mintime.defined() || (mdtime.defined() && mdtime < mintime))
			{
				minmd = i;
				mintime = mdtime;
			}
		}
		// When there's nothing more to read, we exit
		if (minmd == -1) break;
		consumer(*readers[minmd].mdbuf.get());
		readers[minmd].mdbuf.pop();
	}

	// Collect all the results
	vector<string> errors;
	for (size_t i = 0; i < datasets.size(); ++i)
		if (void* res = readers[i].join())
			errors.push_back((const char*)res);
	
	if (!errors.empty())
		throw wibble::exception::Consistency("running metadata queries on multiple datasets", str::join(errors.begin(), errors.end(), "; "));
}

void Merged::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	// Handle the trivial case of only one dataset
	if (datasets.size() == 1)
	{
		datasets[0]->querySummary(matcher, summary);
		return;
	}

	SummaryReader readers[datasets.size()];

	// Start all the readers
	for (size_t i = 0; i < datasets.size(); ++i)
	{
		readers[i].init(matcher, *datasets[i]);
		readers[i].start();
	}

	// Collect all the results
	vector<string> errors;
	for (size_t i = 0; i < datasets.size(); ++i)
	{
		if (void* res = readers[i].join())
			errors.push_back((const char*)res);
		else
			summary.add(readers[i].summary);
	}
	
	if (!errors.empty())
		throw wibble::exception::Consistency("running summary queries on multiple datasets", str::join(errors.begin(), errors.end(), "; "));
}

void Merged::queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype, const std::string& param)
{
	// Here we must serialize, as we do not know how to merge raw data streams
	for (std::vector<ReadonlyDataset*>::iterator i = datasets.begin();
			i != datasets.end(); ++i)
		(*i)->queryBytes(matcher, out, qtype, param);
}

}
}
// vim:set ts=4 sw=4:
