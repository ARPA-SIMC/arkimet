Arkimet formatters
==================

See arkimet.formatter module for example of formatters.

The arkimet.formatter.Formatter class will try all registered formatters in
order, taking the result of the first one that returns non-None.

You can add `.py` files that register custom formatters here, and they can
return None to fall back on the existing ones.
