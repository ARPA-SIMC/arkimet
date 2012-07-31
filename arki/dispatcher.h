#ifndef ARKI_DISPATCHER_H
#define ARKI_DISPATCHER_H

/*
 * dispatcher - Dispatch data into dataset
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

#include <arki/datasetpool.h>
#include <arki/dataset.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <string>
#include <vector>
#include <map>

namespace arki {
class ConfigFile;
class Metadata;

namespace metadata {
class Consumer;
}

class Dispatcher
{
protected:
	// Dispatching information
	std::vector< std::pair<std::string, Matcher> > datasets;
	std::vector< std::pair<std::string, Matcher> > outbounds;

    /// True if we can import another one
    bool m_can_continue;

    /// Number of failed acquires to outbound datasets
    int m_outbound_failures;

    /// Hook called at the beginning of the dispatch procedure for a message.
    virtual void hook_pre_dispatch(Metadata& md);

    /// Hook called with the list of matching datasets for a message.
    virtual void hook_found_datasets(Metadata& md, std::vector<std::string>& found);

    virtual WritableDataset::AcquireResult dispatch_dataset(const std::string& name, Metadata& md) = 0;
    virtual WritableDataset::AcquireResult dispatch_error(Metadata& md);
    virtual WritableDataset::AcquireResult dispatch_duplicates(Metadata& md);

public:
	enum Outcome {
		/// Imported ok
		DISP_OK,
		/// Duplicate, imported in error dataset
		DISP_DUPLICATE_ERROR,
		/// Imported in error dataset for other problems than duplication
		DISP_ERROR,
		/// Had problems, and even writing in the error dataset failed
		DISP_NOTWRITTEN
	};

	Dispatcher(const ConfigFile& cfg);
	virtual ~Dispatcher();

	/**
	 * Return true if the metadata consumer called by the last dispatch()
	 * invocation returned true.
	 */
	bool canContinue() const { return m_can_continue; }

	/**
	 * Return the number of failed acquires to outbound datasets since the
	 * creation of the dispatcher.
	 *
	 * Details on the failures can be found in the notes of the metadata after
	 * acquire.
	 */
	size_t outboundFailures() const { return m_outbound_failures; }

    /**
     * Dispatch the metadata and its data.
     *
     * @returns The outcome of the dispatch.
     */
    Outcome dispatch(Metadata& md, metadata::Consumer& mdc);

    /// Dispatch to error dataset
    void dispatch_error(Metadata& md, metadata::Consumer& mdc);

    virtual void flush() = 0;
};

/**
 * Infrastructure to dispatch metadata into various datasets
 *
 * The metadata will be edited to reflect the data stored inside the target
 * dataset, and sent to the given metadata::Consumer.
 *
 * If there are outbound datasets, a different metadata can be sent to
 * the consumer for every output dataset that accepted it.
 *
 * The metadata sent to the consumer is just a reference to the metadata on
 * input, that will be changed during the dispatch process.  If you are
 * storing the metadata instead of processing them right away, remember to
 * store a copy.
 */
class RealDispatcher : public Dispatcher
{
protected:
    WritableDatasetPool pool;

    // Error dataset
    WritableDataset* dserror;

    // Duplicates dataset
    WritableDataset* dsduplicates;

    virtual WritableDataset::AcquireResult dispatch_dataset(const std::string& name, Metadata& md);
    virtual WritableDataset::AcquireResult dispatch_error(Metadata& md);
    virtual WritableDataset::AcquireResult dispatch_duplicates(Metadata& md);

public:
	RealDispatcher(const ConfigFile& cfg);
	~RealDispatcher();

	/**
	 * Flush all dataset data do disk
	 */
	void flush();
};

/**
 * Test dispatcher implementation.
 *
 * Nothing will happen, except that information about what would have
 * happened on a real dispatch will be sent to the given output stream.
 *
 * @returns The outcome of the dispatch.
 */
class TestDispatcher : public Dispatcher
{
protected:
	const ConfigFile& cfg;
	std::ostream& out;
	size_t m_count;
    std::string prefix;
    std::map<std::string, std::string> m_seen;

    virtual void hook_pre_dispatch(Metadata& md);
    virtual void hook_found_datasets(Metadata& md, std::vector<std::string>& found);
    virtual WritableDataset::AcquireResult dispatch_dataset(const std::string& name, Metadata& md);

public:
	TestDispatcher(const ConfigFile& cfg, std::ostream& out);
	~TestDispatcher();

	/**
	 * Flush all dataset data do disk
	 */
	void flush();
};

}

// vim:set ts=4 sw=4:
#endif
