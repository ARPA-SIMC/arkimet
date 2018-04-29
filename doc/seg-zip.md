# zip segments

zip segments are like [dir](seg-dir.md) segments stored inside a zip file.

The dataset is compressed, but because zip archives compress each file
individually, it is not effective when data stored is small, like in the case
of VM2 or tiny BUFR data.
