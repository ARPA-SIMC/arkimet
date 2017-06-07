# dataset.py - Handle xgribarch datasets
#
# Copyright (C) 2007  ARPAE-SIMC <simc-urp@arpae.it>
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

from metadata import metadata
from matcher import Matcher
from datetime import datetime, timedelta
from pysqlite2 import dbapi2 as sqlite
import md5, subprocess
import os.path

class Dataset:
    """
    Archived data is stored in datasets.

    A dataset is a group of data with similar properties, like the output of a
    given forecast model, or images from a given satellite.

    Every dataset has a dataset name.

    Every data inside a dataset has a data identifier (or data ID) that is
    guaranteed to be unique inside the dataset.  One can obtain a unique
    identifier on a piece of data across all datasets by simply prepending the
    dataset name to the data identifier.

    Every dataset has a dataset index to speed up retrieval of subsets of
    informations from it.

    The dataset index needs to be simple and fast, but does not need to support
    a complex range of queries.

    The data is stored in the dataset without alteration from its original
    form.

    It must be possible to completely regenerate the dataset index by
    rescanning allthe data stored in the dataset.

    A consequence of the last two points is that a dataset can always be fully
    regenerated by just reimporting data previously extracted from it.

    A dataset must be fully independent from other datasets, and not must not hold
    any information about them.

    A dataset should maintain a summary of its contents, that lists the
    various properties of the data it contains.

    Since the dataset groups data with similar properties, the summary is intended
    to change rarely.

    An archive index can therefore be built, indexing all dataset summaries,
    to allow complex data searches across datasets.
    """

    def __init__(self, name, path, conf):
        """
        Initialise the dataset with the information from the 'desc' dict
        """
        self.name = name
        self.path = path
        self.type = conf['path']

    def _store(self, md, dest, sourcedir="."):
        p = subprocess.Popen(["store-grib", "-o", dest, "-s", sourcedir, "-"], stdin=subprocess.PIPE)
        md.write(p.stdin)
        p.stdin.close()
        ret = p.wait()
        if ret != 0:
            raise RuntimeError("Storing the data in the archive, store-grib returned error code %d" % ret)

    def accepts(self, md):
        """
        Return true if this dataset would accept the item described by the
        given metadata.
        """
        # Don't check for duplicate ID, only check if the metadata would be fit
        # for this dataset
        return False

    def contains(self, id):
        """
        Return true if this dataset contains the item with the given unique ID
        """
        return False

    def acquire(self, md):
        """
        Acquire the given metadata item (and related data) in this dataset
        """
        pass

class SimpleDataset(Dataset):
    """
    Basic Dataset implementation.  Still not ready for final use, but it can be
    used as a prototype to build real datasets.
    """
    def __init__(self, name, path, conf):
        Dataset.__init__(self, name, path, conf)
        if not "filter" in conf:
            raise RuntimeError("'filter' not found in the configuration of dataset '%s'" % self.name)
        self.filter = Matcher(conf["filter"])
        needsInit = os.path.exists(self.path + "/index.sqlite")
        self.db = sqlite.connect(self.path + "/index.sqlite")
        if needsInit:
            cur = self.db.cursor()
            cur.execute("""
                create table items (
                    id integer primary key,
                    uid varchar(255) not null,
                    unique(uid)
                )
                """)

    def id(self, md):
        """
        Compute the unique ID of a metadata in this dataset
        """
        o = md.get_origins()
        t = md.get_reference_time_info()
        return ":".join(o + (str(t),))

    def accepts(self, md):
        return self.filter.match(md)

    def contains(self, id):
        cur = self.db.cursor()
        cur.execute("select id from items where uid = ?", id)
        return cur.fetchone() != None

    def acquire(self, md):
        # Daily filename so far
        filename = datetime.now().strftime("%Y%m%d")
        dest = self.path + "/" + filename
        id = self.id(md)
        cur = self.db.cursor()
        # This will throw an exception if it's a duplicate insert
        cur.execute("insert into items (uid) values (?)", id)
        md.set_dataset(self.name, id)
        try:
            self._store(md, dest)
        except:
            cur.execute("delete from items where uid = ?", id)
            raise



class ErrorDataset(Dataset):
    """
    Dataset to hold all data that could not be handled by other datasets
    """
    def __init__(self, name, path, conf):
        Dataset.__init__(self, name, path, conf)
        if not os.path.isdir(self.path):
            raise RuntimeError(self.path + " is not a directory")

    def id(self, md):
        """
        Compute the ID of a metadata in the system using the MD5 sum.

        This should only be done by ErrorDataset, as just adding a comment or
        reformatting the metadata would give a different ID.
        """
        return md5.md5(str(md)).hexdigest()

    def accepts(self, md):
        return True

    def contains(self, id):
        return False

    def acquire(self, md):
        filename = datetime.now().strftime("%Y%m%d")
        dest = self.path + "/" + filename
        md.set_dataset(self.name, self.id(md))
        self._store(md, dest)

def create(path):
    name = os.path.basename(path)
    if not os.path.exists(path + "/config")
        return None

    conf = ConfigObj(path + "/config")
    type = config["type"]
    if type == "error":
        return ErrorDataset(name, path, config)
    elif type == "test":
        return SimpleDataset(name, path, config)
    else:
        raise ValueError("'%s' is not a valid dataset type" % type)


if __name__ == "__main__":
    import sys
    import unittest

    verbose = ("--verbose" in sys.argv)
    #print >>sys.stderr, verbose

    class Test(unittest.TestCase):
        def testOrigins(self):
            origins = [
                ( "GRIB", 1, 2, 3 ),
                ( "GRIB", 4, 3, 2 ),
                ( "GRIB", 6, 3, 1 ) ]
            self.md.set_origins(origins)
            origins1 = self.md.get_origins()
            self.assertEqual(origins, origins1)
            if verbose: self.md.write(sys.stderr)

    #   def testReferenceTimeInfo(self):
    #       time = datetime(2002, 9, 8, 7, 6, 5)
    #       self.md.set_reference_time_info(time)
    #       time1 = self.md.get_reference_time_info()
    #       self.assertEqual([time], time1)

    #       timea = datetime(2002, 6, 5, 4, 3, 2)
    #       timeb = datetime(2002, 7, 6, 5, 4, 3)
    #       self.md.set_reference_time_info((timea, timeb))
    #       if verbose: self.md.write(sys.stderr)
    #       times = self.md.get_reference_time_info()
    #       self.assertEqual(len(times), 1)
    #       timea1, timeb1 = times[0]

    #       self.assertEqual(timea, timea1)
    #       self.assertEqual(timeb, timeb1)

    #   def testNotes(self):
    #       self.md.add_note("this is a test note")
    #       self.md.add_note("this is another test note")
    #       notes = self.md.get_notes()
    #       self.assertEqual(len(notes), 2)
    #       self.assert_( (datetime.now() - notes[0][0]) < timedelta(hours = 1))
    #       self.assertEqual(notes[0][1], "this is a test note")
    #       self.assert_( (datetime.now() - notes[1][0]) < timedelta(hours = 1))
    #       self.assertEqual(notes[1][1], "this is another test note")
    #       if verbose: self.md.write(sys.stderr)

    unittest.main()

