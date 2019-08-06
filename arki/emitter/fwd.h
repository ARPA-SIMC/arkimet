#ifndef ARKI_EMITTER_FWD_H
#define ARKI_EMITTER_FWD_H

namespace arki {
struct Emitter;

namespace emitter {
struct Keys;
extern const Keys keys_json;
extern const Keys keys_python;

struct Reader;

namespace memory {
struct Node;
struct List;
struct Mapping;
}

}
}

#endif
