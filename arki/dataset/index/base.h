#ifndef ARKI_DATASET_INDEX_BASE_H
#define ARKI_DATASET_INDEX_BASE_H

#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/utils/sqlite.h>
#include <string>
#include <set>
#include <sstream>

namespace arki {
class Metadata;
class Matcher;

namespace dataset {
namespace index {

/**
 * Convert a [comma and optional spaces]-separated string with metadata
 * component names into a MetadataComponents bitfield
 */
std::set<types::Code> parseMetadataBitmask(const std::string& components);

/**
 * Configurable creator of unique database IDs from metadata
 */
struct IDMaker
{
	std::set<types::Code> components;
	
	IDMaker() {}
	IDMaker(const std::set<types::Code>& components) : components(components) {}

	std::string id(const Metadata& md) const;
};


// Exception we can throw if an element was not found
struct NotFound {};

/**
 * Format a vector of ints to a SQL match expression.
 *
 * If the vector has only one item, it becomes =item.
 *
 * If the vector has more than one item, it becomes IN(val1,val2...)
 */
std::string fmtin(const std::vector<int>& vals);

}
}
}

// vim:set ts=4 sw=4:
#endif
