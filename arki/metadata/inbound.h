#ifndef ARKI_METADATA_INBOUND_H
#define ARKI_METADATA_INBOUND_H

#include <arki/defs.h>
#include <arki/metadata/fwd.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

namespace arki::metadata {

/**
 * Track the ingestion process of a metadata element and its associated data
 */
class Inbound
{
public:
    /// Possible ingestion outcomes
    enum class Result {
        /// Ingestion successful
        OK,
        /// Ingestion failed because the data is already present
        DUPLICATE,
        /// Ingestion failed for other reasons
        ERROR,
    };

    /// Metadata to acquire
    std::shared_ptr<Metadata> md;

    /// Ingestion result
    Result result = Result::ERROR;

    /**
     * Name of the destination where it has been acquired (empty when not
     * acquired)
     */
    std::string destination;

    /**
     * Messages collected during ingestion
     */
    std::vector<std::string> log;

    explicit Inbound(std::shared_ptr<Metadata> md);
    ~Inbound();
    Inbound(const Inbound& o)            = delete;
    Inbound(Inbound&& o)                 = delete;
    Inbound& operator=(const Inbound& o) = delete;
    Inbound& operator=(Inbound&& o)      = delete;
};

std::ostream& operator<<(std::ostream& o, Inbound::Result res);

/**
 * Batch of inbound data to import.
 */
class InboundBatch : private std::vector<std::shared_ptr<Inbound>>
{
public:
    using std::vector<std::shared_ptr<Inbound>>::vector;
    using std::vector<std::shared_ptr<Inbound>>::size;
    using std::vector<std::shared_ptr<Inbound>>::empty;
    using std::vector<std::shared_ptr<Inbound>>::operator[];
    using std::vector<std::shared_ptr<Inbound>>::const_iterator;
    using std::vector<std::shared_ptr<Inbound>>::begin;
    using std::vector<std::shared_ptr<Inbound>>::end;
    using std::vector<std::shared_ptr<Inbound>>::emplace_back;
    using std::vector<std::shared_ptr<Inbound>>::clear;

    /**
     * Add a metadata to the batch
     */
    void add(std::shared_ptr<Metadata> md);

    /**
     * Set all elements in the batch to ACQ_ERROR
     */
    void set_all_error(const std::string& note);
};

} // namespace arki::metadata

#endif
