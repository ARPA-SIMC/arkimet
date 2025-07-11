#include "matcher.h"
#include "core/time.h"
#include "matcher/utils.h"
#include <memory>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {

Matcher::Matcher(std::unique_ptr<matcher::AND>&& impl) : m_impl(move(impl)) {}
Matcher::Matcher(std::shared_ptr<matcher::AND> impl) : m_impl(impl) {}

bool Matcher::empty() const { return m_impl.get() == 0 || m_impl->empty(); }

const matcher::AND* Matcher::operator->() const
{
    if (!m_impl.get())
        throw std::runtime_error("cannot access matcher: matcher is empty");
    return m_impl.get();
}

std::string Matcher::name() const
{
    if (m_impl.get())
        return m_impl->name();
    return std::string();
}

bool Matcher::operator()(const types::Type& t) const
{
    if (m_impl.get())
        return m_impl->matchItem(t);
    // An empty matcher always matches
    return true;
}

bool Matcher::operator()(const types::ItemSet& md) const
{
    if (m_impl.get())
        return m_impl->matchItemSet(md);
    // An empty matcher always matches
    return true;
}

bool Matcher::operator()(const Metadata& md) const
{
    if (m_impl.get())
        return m_impl->matchMetadata(md);
    // An empty matcher always matches
    return true;
}

bool Matcher::operator()(const core::Interval& interval) const
{
    if (m_impl.get())
        return m_impl->match_interval(interval);
    // An empty matcher always matches
    return true;
}

bool Matcher::operator()(types::Code code, const uint8_t* data,
                         unsigned size) const
{
    if (m_impl.get())
        return m_impl->match_buffer(code, data, size);
    // An empty matcher always matches
    return true;
}

std::shared_ptr<matcher::OR> Matcher::get(types::Code code) const
{
    if (m_impl)
        return m_impl->get(code);
    return nullptr;
}

void Matcher::foreach_type(
    std::function<void(types::Code, const matcher::OR&)> dest) const
{
    if (!m_impl)
        return;
    return m_impl->foreach_type(dest);
}

std::string Matcher::toString() const
{
    if (m_impl)
        return m_impl->toString();
    return std::string();
}

std::string Matcher::toStringExpanded() const
{
    if (m_impl)
        return m_impl->toStringExpanded();
    return std::string();
}

void Matcher::split(const std::set<types::Code>& codes, Matcher& with,
                    Matcher& without) const
{
    if (!m_impl)
    {
        with    = Matcher();
        without = Matcher();
    }
    else
    {
        // Create the empty matchers and assign them right away, so we sort out
        // memory management
        unique_ptr<matcher::AND> awith(new matcher::AND);
        unique_ptr<matcher::AND> awithout(new matcher::AND);

        m_impl->split(codes, *awith, *awithout);

        if (awith->empty())
            with = Matcher();
        else
            with = Matcher(move(awith));

        if (awithout->empty())
            without = Matcher();
        else
            without = Matcher(move(awithout));
    }
}

bool Matcher::intersect_interval(core::Interval& interval) const
{
    shared_ptr<matcher::OR> reftime;

    // We have nothing to match: we match the open range
    if (!m_impl)
        return true;

    reftime = m_impl->get(TYPE_REFTIME);

    // We have no reftime to match: we match the open range
    if (!reftime)
        return true;

    if (!reftime->intersect_interval(interval))
        return false;

    return true;
}

Matcher Matcher::merge(const Matcher& m) const
{
    if (m_impl && m.m_impl)
    {
        shared_ptr<matcher::AND> result(m_impl->clone());
        result->merge(*m.m_impl);
        return Matcher(result);
    }
    else
        return Matcher();
}

Matcher Matcher::update(const Matcher& m) const
{
    if (m_impl && m.m_impl)
    {
        shared_ptr<matcher::AND> result(m_impl->clone());
        result->update(*m.m_impl);
        return Matcher(result);
    }
    else if (m_impl)
    {
        shared_ptr<matcher::AND> result(m_impl->clone());
        return Matcher(result);
    }
    else if (m.m_impl)
    {
        shared_ptr<matcher::AND> result(m.m_impl->clone());
        return Matcher(result);
    }
    else
        return Matcher();
}

Matcher Matcher::for_interval(const core::Interval& interval)
{
    return matcher::AND::for_interval(interval);
}

Matcher Matcher::for_month(unsigned year, unsigned month)
{
    core::Time begin(year, month);
    core::Time end(begin.start_of_next_month());
    return for_interval(core::Interval(begin, end));
}

std::ostream& operator<<(std::ostream& o, const Matcher& m)
{
    return o << m.toString();
}

} // namespace arki
