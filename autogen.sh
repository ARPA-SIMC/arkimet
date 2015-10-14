#!/bin/sh -x
set -e
# Downloading wibble
wget -O embedded/wibble.tar.gz http://people.debian.org/~enrico/wibble/embeddable/wibble.tar.gz
tar -C embedded -zxf embedded/wibble.tar.gz
# Generating the ./configure file
autoreconf -ifv
