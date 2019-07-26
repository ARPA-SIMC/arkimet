#ifndef ARKI_DISPATCHER_H
#define ARKI_DISPATCHER_H

#include <arki/datasets.h>
#include <arki/dataset.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <string>
#include <vector>
#include <map>

namespace arki {
class Metadata;
class Validator;

class Dispatcher
{
protected:
	// Dispatching information
	std::vector< std::pair<std::string, Matcher> > datasets;
	std::vector< std::pair<std::string, Matcher> > outbounds;
    std::vector<const Validator*> validators;

    /// True if we can import another one
    bool m_can_continue;

    /// Number of failed acquires to outbound datasets
    int m_outbound_failures;

    virtual void raw_dispatch_dataset(const std::string& name, dataset::WriterBatch& batch, bool drop_cached_data_on_commit) = 0;

public:
    Dispatcher(const core::cfg::Sections& cfg);
    virtual ~Dispatcher();

    /**
     * Add a validator to this dispatcher.
     *
     * Memory management is handled by the caller, so the validator must be
     * valid during the whole lifetime of the dispatcher.
     */
    void add_validator(const Validator& v);

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
     * Dispatch the metadata and their data.
     *
     * @returns The outcome of the dispatch, one element per metadata in mds.
     */
    virtual void dispatch(dataset::WriterBatch& batch, bool drop_cached_data_on_commit);

    virtual void flush() = 0;
};

/**
 * Infrastructure to dispatch metadata into various datasets
 *
 * The metadata will be edited to reflect the data stored inside the target
 * dataset, and sent to the given metadata_dest_func.
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
    Datasets datasets;
    WriterPool pool;

    void raw_dispatch_dataset(const std::string& name, dataset::WriterBatch& batch, bool drop_cached_data_on_commit) override;

public:
    RealDispatcher(const core::cfg::Sections& cfg);
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
    const core::cfg::Sections& cfg;

    void raw_dispatch_dataset(const std::string& name, dataset::WriterBatch& batch, bool drop_cached_data_on_commit) override;

public:
    TestDispatcher(const core::cfg::Sections& cfg);
    ~TestDispatcher();

    void dispatch(dataset::WriterBatch& batch, bool drop_cached_data_on_commit) override;

    /**
     * Flush all dataset data do disk
     */
    void flush();
};

}
#endif
