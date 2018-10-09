#include "config.h"

#include <arki/utils/accounting.h>

using namespace std;

namespace arki {
namespace utils {
namespace acct {

Counter plain_data_read_count("Plain data read count");
Counter gzip_data_read_count("Gzip data read count");
Counter gzip_forward_seek_bytes("Gzip forward seek bytes");
Counter gzip_idx_reposition_count("Gzip index reposition count");
Counter acquire_single_count("Count of dataset acquire operations on single metadata");
Counter acquire_batch_count("Count of dataset acquire operations on metadata batches");

}
}
}
