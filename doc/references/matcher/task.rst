Matching ODIMH5 Task
====================

Syntax: ``task:value``

Matches if values is found inside the metadata.

If value is empty, any data with task attribute is matched.

Examples
^^^^^^^^

Given task ``prova 123``:

* ``task:`` matches
* ``task:prova`` matches
* ``task:123`` matches
* ``task:aaaaaaaaaaaaaaaa`` does not match
* ``task:prova123`` does not match

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. doctest matched: 3
.. doctest not_matched: 2
