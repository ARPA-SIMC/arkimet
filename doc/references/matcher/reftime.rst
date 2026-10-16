Matching reference times
========================

Syntax: ``reftime:<operator><date>[,<operator><date>...]``

A reftime matcher is a sequence of expressions separated by a comma. All the
expressions are ANDed together.

Valid operators are: ``>=``, ``>``, ``<=``, ``<``, ``==``, ``=``.

For example: ``reftime:=2010-05-03 12:00:00`` matches midday on the 3rd of May.

A date can be specified partially, and it will be considered as a time
interval. For example, ``reftime:=2007-01`` matches from midnight of the 1st of
January 2007 to 23:59:59 of the 31st of January 2007, inclusive.

It is also possible to specify the time, without the date: ``>=12``, ``>=12:00``
and ``>=12:00:00`` all mean "between midday and midnight of any day". ``>12`` means
"from 13:00:00 until midnight". ``=12`` means "from 12:00:00 to 12:59:59".

Finally, it is possible to specify a time step: ``>=2007-06-05 04:00:30%12h``
means "anything from 04:00:30 June 5, 2007, but only at precisely 04:00:30 and
18:00:30".

The time step can be given in hours, minutes, seconds or any of their
combinations. For example: ``12h``, ``30m``, ``12h30m15s``.

Dates and times can also be specified relative to the curren time:

* ``now``: current date and time
* ``today``: current date

There are also aliases for commonly used bits:

* ``reftime:=yesterday`` is equivalent to ``reftime:=today - 1 day``
* ``reftime:=tomorrow``  is equivalent to ``reftime:=today + 1 day``
* ``reftime:=midday``    is equivalent to ``reftime:=12:00:00``
* ``reftime:=noon``      is equivalent to ``reftime:=12:00:00``
* ``reftime:=midnight``  is equivalent to ``reftime:=00:00:00``

Dates can be altered using ``+``, ``-``, ``ago``, ``before``, ``after``.

For example:

* ``reftime:=2 days before yesterday`` is equivalent to ``reftime:=today - 3 days``
* ``reftime:=1 hour and 30 minutes after tomorrow midnight`` is equivalent to ``reftime:=tomorrow 01:30:00``
* ``reftime:=1 hour after today midday`` is equivalent to ``reftime:=today 13:00:00``
* ``reftime:=processione san luca 2010`` is equivalent to ``reftime:=34 days after easter 2010``

You can use ``arki-dump --query 'reftime:...'`` to check how a query gets parsed.

Examples
^^^^^^^^

Given reftime ``2010-09-08 07:06:05``:

Year intervals:

* ``reftime:>=2010`` matches
* ``reftime:<=2010`` matches
* ``reftime:==2010`` matches
* ``reftime:>2009`` matches
* ``reftime:<2011`` matches
* ``reftime:<2010`` does not match
* ``reftime:>2009,<2011`` matches
* ``reftime:>2009,<2010`` does not match

Month intervals:

* ``reftime:>=2010-09`` matches
* ``reftime:<=2010-09`` matches
* ``reftime:==2010-09`` matches
* ``reftime:>2010-08`` matches
* ``reftime:<2010-10`` matches
* ``reftime:<2010-09`` does not match
* ``reftime:>2010-08,<2010-10`` matches
* ``reftime:>2010-08,<2010-09`` does not match

Day intervals:

* ``reftime:>=2010-09-08`` matches
* ``reftime:<=2010-09-08`` matches
* ``reftime:==2010-09-08`` matches
* ``reftime:>2010-09-07`` matches
* ``reftime:<2010-09-09`` matches
* ``reftime:<2010-09-08`` does not match
* ``reftime:>2010-09-07,<2010-09-09`` matches
* ``reftime:>2010-09-07,<2010-09-08`` does not match

Hour intervals:

* ``reftime:>=2010-09-08 07`` matches
* ``reftime:<=2010-09-08 07`` matches
* ``reftime:==2010-09-08 07`` matches
* ``reftime:>2010-09-08 06`` matches
* ``reftime:<2010-09-08 08`` matches
* ``reftime:<2010-09-08 07`` does not match
* ``reftime:>2010-09-08 06,<2010-09-08 08`` matches
* ``reftime:>2010-09-08 06,<2010-09-08 07`` does not match

Minute intervals:

* ``reftime:>=2010-09-08 07:06`` matches
* ``reftime:<=2010-09-08 07:06`` matches
* ``reftime:==2010-09-08 07:06`` matches
* ``reftime:>2010-09-08 07:05`` matches
* ``reftime:<2010-09-08 07:07`` matches
* ``reftime:<2010-09-08 07:06`` does not match
* ``reftime:>2010-09-08 07:05,<2010-09-08 07:07`` matches
* ``reftime:>2010-09-08 07:05,<2010-09-08 07:06`` does not match

Precise timestamps:

* ``reftime:>=2010-09-08 07:06:05`` matches
* ``reftime:<=2010-09-08 07:06:05`` matches
* ``reftime:==2010-09-08 07:06:05`` matches
* ``reftime:>2010-09-08 07:06:04`` matches
* ``reftime:<2010-09-08 07:06:06`` matches
* ``reftime:<2010-09-08 07:06:05`` does not match
* ``reftime:>2010-09-08 07:06:04,<2010-09-08 07:06:06`` matches
* ``reftime:>2010-09-08 07:05:04,<2010-09-08 07:06:05`` does not match

Hour expressions:

* ``reftime:=07`` matches
* ``reftime:>06`` matches
* ``reftime:<08`` matches
* ``reftime:>07`` does not match: same as ``>=08``
* ``reftime:=07:06`` matches
* ``reftime:>07:05`` matches
* ``reftime:<08:00`` matches
* ``reftime:>07:06`` does not match: same as ``>=07:07``
* ``reftime:=07:06:05`` matches
* ``reftime:>07:06:04`` matches
* ``reftime:<07:06:06`` matches
* ``reftime:>07:06:05`` does not match
* ``reftime:<07:06:05`` does not match

Time steps:

* ``reftime:>=2010-09-08 %5s`` matches
* ``reftime:%5s`` matches
* ``reftime:%2s`` does not match

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. doctest matched: 47
.. doctest not_matched: 17
