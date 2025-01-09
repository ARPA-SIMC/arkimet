#ifndef ARKI_SEGMENT_BASE_H
#define ARKI_SEGMENT_BASE_H

#include <arki/segment/data.h>
#include <arki/types/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/scan/fwd.h>
#include <string>
#include <vector>
#include <functional>
#include <type_traits>

namespace arki::segment::data {

template<typename Data>
struct BaseReader : public data::Reader
{
    static_assert(std::is_base_of_v<arki::segment::Data, Data>, "Data not derived from arki::segment::Data");
protected:
    std::shared_ptr<const Data> m_data;

public:
    BaseReader(std::shared_ptr<const Data> data, std::shared_ptr<core::Lock> lock)
        : data::Reader(lock), m_data(data)
    {
    }

    const Segment& segment() const override { return data().segment(); }
    const Data& data() const override { return *m_data; }
};

template<typename Data>
struct BaseWriter : public data::Writer
{
    static_assert(std::is_base_of_v<arki::segment::Data, Data>, "Data not derived from arki::segment::Data");
protected:
    std::shared_ptr<const Data> m_data;

public:
    BaseWriter(const data::WriterConfig& config, std::shared_ptr<const Data> data)
        : data::Writer(config), m_data(data)
    {
    }

    const Segment& segment() const override { return data().segment(); }
    const Data& data() const override { return *m_data; }
};

template<typename Data>
struct BaseChecker : public data::Checker
{
    static_assert(std::is_base_of_v<arki::segment::Data, Data>, "Data not derived from arki::segment::Data");
protected:
    std::shared_ptr<const Data> m_data;

public:
    BaseChecker(std::shared_ptr<const Data> data) : m_data(data)
    {
    }

    const Segment& segment() const override { return data().segment(); }
    const Data& data() const override { return *m_data; }
    // std::shared_ptr<data::Checker> checker_moved(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) const override;

    std::shared_ptr<Checker> move(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath) override;
};

}

#endif
