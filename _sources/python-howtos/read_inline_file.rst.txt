.. _python_how_read_inline_file:

Read the output of ``arki-query --inline``
==========================================

The output of ``arki-query --inline`` is the same as is given as standard input
to arkimet processors, so this recipe can be used both to work on exported
data, and to implement arikmet processors.

The examples that follow use a placeholder ``process_metadata`` function to act
on the parsed metadata. Here's an example of such a function::

    counter = 0
    def process_metadata(md: arkimet.Metadata):
        counter += 1
        print(f"Received metadata #{counter}")


Reading as a dataset
--------------------

Note: this only works if the data is saved on a real file.

This creates an ``arkimet.Dataset`` object that accesses the file::

    ds = arki.dataset.Reader({
        "format": "arkimet",
        "name": "input",
        "path": filename,
        "type": "file",
    })

You can then iterate over it::

    for md in ds.query_data(...):
        process_medatata(md)

Or let arkimet do the iteration by passing a callback::

    ds.query_data(on_metadata=process_metadata)

Arkimet's python bindings do not support streaming iteration over results: in
the first case, ``ds.query_data`` will return a list of metadata after scanning
the whole file, while in the second case the callback is called on metadata as
the file is being read.


Reading from standard input
---------------------------

Standard input does not allow seeking, which would be required to access it as
a dataset. ``arkimet.Metadata.read_bundle`` can be used to read it
sequentially::

    arkimet.Metadata.read_bundle(sys.stdin.buffer, dest=process_metadata)
