#ifndef ARKI_METADATA_INBOUND_H
#define ARKI_METADATA_INBOUND_H

#include <arki/metadata/fwd.h>
#include <memory>
#include <string>
#include <vector>
#include <iosfwd>

namespace arki::metadata {

/**
 * Track the ingestion process of a metadata element and its associated data
 */
class Inbound
{
public:
    /// Possible ingestion outcomes
    enum class Result
    {
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
    Inbound(const Inbound& o) = delete;
    Inbound(Inbound&& o) = delete;
    Inbound& operator=(const Inbound& o) = delete;
    Inbound& operator=(Inbound&& o) = delete;
};

std::ostream& operator<<(std::ostream& o, Inbound::Result res);


class InboundBatch : public std::vector<std::shared_ptr<Inbound>>
{
public:
    /**
     * Set all elements in the batch to ACQ_ERROR
     */
    void set_all_error(const std::string& note);
};

}

#endif
