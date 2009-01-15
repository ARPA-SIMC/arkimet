#!/usr/bin/python

# Merge arkimet dataset configuration
#
# Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Author: Enrico Zini <enrico@enricozini.com>

import sys
from configobj import ConfigObj
import os.path

# Read metadata from stdin and dispatch them to the dataset that wants them

if len(sys.argv) == 1:
    print >>sys.stderr, "Usage:", sys.argv[0], "dir1 [dir2 ...]"
    print >>sys.stderr, "Reads dataset configuration from the given directories, merges them"
    print >>sys.stderr, "and outputs the merged config file to standard output."
    sys.exit(1)

# Parse configuration
config = ConfigObj(list_values = False)

for dir in sys.argv[1:]:
    if not os.path.isdir(dir):
        print >>sys.stderr, "%s is not a directory: skipped" % dir
        continue
    if not os.path.exists(dir + "/config"):
        print >>sys.stderr, "%s/config does not exist: skipped" % dir
        continue
    name = os.path.basename(dir)
    try:
        dsconfig = ConfigObj(dir + "/config", list_values = False)
        dsconfig["name"] = name
        dsconfig["path"] = os.path.abspath(dir)
        config[name] = dict()
        config[name].merge(dsconfig)
    except:
	exc = sys.exc_info()[1]
        print >>sys.stderr, "%s skipped: %s" % (dir, exc)

config.write(sys.stdout)
print >>sys.stdout
