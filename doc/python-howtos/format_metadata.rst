.. _python_how_format_metadata:

Formatting the contents of metadata
===================================

One can do in python the same as ``--annotate`` does to ``arki-query`` output,
with more flexibility.

The functionality is in the :class:`arkimet.Formatter` class, which has a
:meth:`arkimet.Formatter.format` method that takes a metadata item and returns its
description as a string::

    formatter = arkimet.Formatter()
    print("Area:", formatter.format(mds[0].to_python("area")))
    print("Level:", formatter.format(mds[0].to_python("level")))


Writing annotated metadata
--------------------------

You can use ``annotate=True`` when calling :meth:`arkimet.Metadata.write` and
to write JSON and YAML with metadata item descriptions.

You can use ``annotate=True`` when calling :meth:`arkimet.Summary.write` and
:meth:`arkimet.Summary.write_short` to write JSON and YAML with metadata item
descriptions.


Implementing new formatters
---------------------------

You can install ``.py`` files in ``/etc/arkimet/conf/format/`` to add your own
formatting functions system-wide, or register them using
:meth:`arkimet.formatter.Formatter.register`.

For example, this installs a formatter for an example area of type ``CUSTOM``::

    from arkimet.formatter import Formatter
    import arkimet as arki


    def format_area(v):
        if v["style"] == "GRIB":
            values = v.get("val")
            # values is now a dict
            if values.get("type") != "CUSTOM":
                return None
            return f"{values['field1']}x{values['field2']}"

        # Return None by default to fall back to the other Area formatters
        return None


    Formatter.register("area", format_area)
