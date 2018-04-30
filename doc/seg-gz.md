# gz and gzidx segments

Gzipped segments are [concat](seg-concat.md) segments that have been compressed
with `gzip`. Because this compression processes all data together, it can
exploit similarities between different data, and compress efficiently long
sequences of VM2 or small BUFR data.

While compressing data together achieves high compression ratios, it means that
to get even just one item, the whole compressed stream needs to be decompressed.

This can only be a problem if segments are very large: in that case, arkimet
can restart the compressed stream each so many items, and keep an index of
where each stream begins. This means that to extract one data item, there is no
need to decompress the whole file, but only the compressed chunk where the item
is stored.

The group size can be controlled by the `gz group size` setting in the dataset,
which defaults to 512. A value of 0 disables the grouping feature and
compresses all the data in the segment together, without writing an index file.

Higher group sizes mean higher compression ratios, and smaller group sizes
means smaller partial reads when only a small part of the segment needs to be
read. This configuration is highly dependeny on the specific datasets,
considering:

 * how big is a segment: for small segments, grouping makes little sense;
 * how big are data items: if they are big, compressing them all together makes
   little sense, and you may want a [zip](seg-zip.md) segment instead;
 * what kind of queries are made to the dataset: if queries need most of its
   contents, all needs to be decompressed anyway and grouping makes little
   sense;
 * what kind of storage is used to store the segment: an SSD with no seek time
   would make grouping more convenient, while a sequential disk with high seek
   times might require more time in the seeks needed to read the index than it
   would take to sequentially read the whole segment. A remote storage which
   would require downloading all the segment from the network to access it
   would also make grouping optimization unnecessary.

If you have very large segments made up of very small data items, which gets
queried in a way that a very few data items from the segment are needed each
time, then tweaking compression grouping could help a lot.
