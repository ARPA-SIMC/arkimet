#ifndef ARKI_READER_REGISTRY_H
#define ARKI_READER_REGISTRY_H

#include <arki/core/file.h>
#include <string>
#include <memory>
#include <unordered_map>

namespace arki {
struct Reader;

namespace reader {

template<typename READER>
class Registry
{
protected:
    unsigned updates_since_last_cleanup = 0;

    std::unordered_map<std::string, std::weak_ptr<Reader>> cache;

    std::shared_ptr<Reader> instantiate(const std::string& abspath)
    {
        std::shared_ptr<core::lock::Policy> lock_policy(new core::lock::OFDPolicy);
        return std::make_shared<READER>(abspath, lock_policy);
    }

public:
    /**
     * Instantiate or reuse a reader
     */
    std::shared_ptr<Reader> reader(const std::string& abspath)
    {
        auto res = cache.find(abspath);
        if (res == cache.end())
        {
            if (updates_since_last_cleanup > 1024)
                cleanup();
            else
                ++updates_since_last_cleanup;
            auto reader = instantiate(abspath);
            cache.insert(make_pair(abspath, reader));
            return reader;
        }

        if (res->second.expired())
        {
            auto reader = instantiate(abspath);
            res->second = reader;
            return reader;
        }

        return res->second.lock();
    }


    /**
     * Remove from the cache the reader for the given file, if it exists.
     *
     * Calls to reader(abspath) after invalidation will instantiate a new
     * reader, even if some reader for this file is still used by some existing
     * metadata.
     *
     * This is useful to make sure that after a repack, new metadata will refer
     * to the new file.
     */
    void invalidate(const std::string& abspath)
    {
        cache.erase(abspath);
    }


    /**
     * Drop everything from the cache, forcing reinstantiation of readers.
     *
     * This is useful after a repack, to revent reusing a reader that points to
     * a file that has been replaced
     */
    void clear()
    {
        cache.clear();
    }


    /**
     * Remove expired entries from the cache
     */
    void cleanup()
    {
        auto i = cache.begin();
        while (i != cache.end())
        {
            if (i->second.expired())
            {
                auto next = i;
                ++next;
                cache.erase(i);
                i = next;
            } else
                ++i;
        }
    }

    /**
     * Return a const reference to the cache, for inspection in tests
     */
    const std::unordered_map<std::string, std::weak_ptr<Reader>>& test_inspect_cache() const { return cache; }
};

}
}

#endif
