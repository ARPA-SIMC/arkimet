Arkimet bounding box algorithms
===============================

See arkimet.bbox module for example of bounding box algorithms.

The arkimet.bbox.BBox class will try all registered algorithms in order, taking
the result of the first one that returns non-None.

You can add `.py` files that register custom bounding box algorithms here, and
they can return None to fall back on the existing ones.
