# tar segments

Tar segment are like [concat](seg-concat.md) segments, but using the tar format
to encapsulate each data item. This has some overhead (from 512 to 1023 bytes
per item), but it allows for fast lookup and it stores data using a well
established standard.

It can also store in a single file data like ODIMH5 which cannot be otherwise
concatenated.

When storing data in tar segments, arkimet tracks the exact offsets in the
`.tar` file where data begins and ends, so it can seek directly to it
regardless of the `.tar` envelope.
