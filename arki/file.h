#include <arki/utils/sys.h>

namespace arki {

/*
 * NamedFileDescriptor and File come from wobble and moving their
 * implementation outside of utils means making it hard to update it in the
 * future.
 *
 * This header exists to promote NamedFileDescriptor and File to non-utils arki
 * namespace, so they can be used by general arkimet APIs.
 */

using arki::utils::sys::NamedFileDescriptor;
using arki::utils::sys::File;

}
