/*
 * dataset/merged - Access many datasets at the same time
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/sort.h>

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
	const DataQuery* query;
	string errorbuf;

	virtual void* main()
	{
		try {
			if (!query)
				throw wibble::exception::Consistency("executing query in subthread", "no query has been set");
			Consumer cons(mdbuf);
			dataset->queryData(*query, cons);
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

	MetadataReader() : dataset(0), query(0) {}

	void init(ReadonlyDataset& dataset, const DataQuery* query)
	{
		this->dataset = &dataset;
		this->query = query;
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

void Merged::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	// Handle the trivial case of only one dataset
	if (datasets.size() == 1)
	{
		datasets[0]->queryData(q, consumer);
		return;
	}

	MetadataReader readers[datasets.size()];

	// Start all the readers
	for (size_t i = 0; i < datasets.size(); ++i)
	{
		readers[i].init(*datasets[i], &q);
		readers[i].start();
	}

	// Output items in time-sorted order or in the order asked by q
	// Note: we assume that every dataset will give us data sorted as q
	// asks, so here we just merge sorted data
	auto_ptr<sort::Compare> cmp;
	const sort::Compare* sorter = q.sorter;
	if (!sorter)
	{
		cmp = sort::Compare::parse("");
		sorter = cmp.get();
	}
	while (true)
	{
		Metadata* minmd = 0;
		int minmd_idx = 0;
		for (size_t i = 0; i < datasets.size(); ++i)
		{
			Metadata* md = readers[i].mdbuf.get();
			if (!md) continue;
			if (!minmd || sorter->compare(*md, *minmd) < 0)
			{
				minmd = md;
				minmd_idx = i;
			}
		}
		// When there's nothing more to read, we exit
		if (minmd == 0) break;
		consumer(*minmd);
		readers[minmd_idx].mdbuf.pop();
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

void Merged::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	// Here we must serialize, as we do not know how to merge raw data streams
	//
	// We cannot just wrap queryData because some subdatasets could be
	// remote, and that would mean doing postprocessing on the client side,
	// potentially transferring terabytes of data just to produce a number

	for (std::vector<ReadonlyDataset*>::iterator i = datasets.begin();
			i != datasets.end(); ++i)
		(*i)->queryBytes(q, out);
}

}
}
// vim:set ts=4 sw=4:
