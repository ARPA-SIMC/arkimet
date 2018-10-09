#ifndef ARKI_UTILS_ACCOUNTING_H
#define ARKI_UTILS_ACCOUNTING_H

#include <stddef.h>

namespace arki {
namespace utils {

/**
 * Simple global counters to keep track of various arkimet functions
 */
namespace acct {

class Counter
{
protected:
	// Use a static const char* (forcing to use of const strings) to keep
	// initialisation simple
	const char* m_name;
	size_t m_val;

public:
	Counter(const char* name) : m_name(name), m_val(0) {}

	/// Increment counter by some amount
	void incr(size_t amount = 1) { m_val += amount; }

	/// Reset counter
	void reset() { m_val = 0; }
		
	/// Get value
	size_t val() const { return m_val; }

	/// Get name
	const char* name() const { return m_name; }
};

extern Counter plain_data_read_count;
extern Counter gzip_data_read_count;
extern Counter gzip_forward_seek_bytes;
extern Counter gzip_idx_reposition_count;
extern Counter acquire_single_count;
extern Counter acquire_batch_count;

}
}
}

#endif
