# metadata.py - Handle xgribarch metadata
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

import libxml2
from datetime import datetime, timedelta
import time
from StringIO import StringIO

GML_NAMESPACE = "http://www.opengis.net/gml"
ARC_NAMESPACE = "http://www.smr.arpa.emr.it/xgribarch"

def _text_contents(node):
    """
    Get the text contents of the first text node contained in the given node
    """
    return node.content

def _from_iso8601(text):
    # FIXME: set timezone as UTC
    return datetime(*time.strptime(text, "%Y-%m-%dT%H:%M:%SZ")[:6])

def _to_iso8601(dt):
    # FIXME: set timezone as UTC
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _datetime_contents(node):
    """
    Get the contents of the first text node contained in the given node, as a
    datetime object.
    """
    if node.nsProp("indeterminatePosition", GML_NAMESPACE) == "now":
        return datetime.now()
    else:
        return _from_iso8601(node.content.strip())

def _iter_elements(node, ns, name):
    """
    Iterate all child notes, nonrecursively, that are Elements and have the
    given tag of the given namespace.
    """
    if node.children == None:
        return
    for el in node.children:
        if el.type != 'element': continue
        if el.ns() != ns: continue
        if el.name != name: continue
        yield el

def _get_element(node, ns, name):
    """
    Return the first element child of node with the given namespace and name.

    Returns None of node does not contain any suitable element.
    """
    res = None
    for el in _iter_elements(node, ns, name):
        res = el
        break
    return res

class Metadata:
    class Header:
        """
        Header used to separate metadata items inside the same file
        """
        def __init__(self):
            self.size = 0

        def read(self, fd):
            """
            Read the header from the given file descriptor
            """
            # We cannot iterate fd, because it will silently read all input and
            # we can't read the XMl data afterwards
            while True:
                line = fd.readline()
                if line == "":
                    # End of file
                    return False
                line = line.strip()
                # Stop reading the header when we get an empty line
                if line == "":
                    return True
                key, val = line.split(": ", 1)
                if key == 'Size':
                    self.size = int(val)
                # Ignore everything we do not understand
            return False

        def write(self, fd):
            """
            Write the header to the given file descriptor
            """
            fd.write("Size: %d\n\n" % self.size)

    def _set_text_contents(self, node, str):
        """
        Replace the text contents of the given node with the given string
        """
        # First remove all text nodes
        for el in node.childNodes:
            if el.nodeType != xml.dom.Node.TEXT_NODE: continue
            el.parentNode.removeChild(el)
        node.appendChild(self.doc.createTextNode(str))

    def _set_datetime_contents(self, node, dt):
        """
        Replace the text contents of the given node with the given string
        formatted to string using UTC ISO-8601
        """
        self._set_text_contents(node, dt.strftime("%Y-%m-%dT%H:%M:%SZ"))

    def _obtainElement(self, parent, ns, name):
        cur = None
        for el in _iter_elements(parent, ns, name):
            #print >>sys.stderr, "ITER", el
            cur = el
            break
        if cur == None:
            cur = parent.newChild(ns, name, None)
        return cur

    def __init__(self):
        self.doc = None

    def __del__(self):
        if self.doc != None:
            # Why, I mean, why must I do this?
            self.doc.freeDoc()

    def __str__(self):
        return str(self.doc)

    def create(self):
        """
        Create an empty Metadata element from scratch
        """
        self.doc = libxml2.newDoc("1.0")
        self.root = self.doc.newChild(None, "md", None)
        self.nsarc = self.root.newNs(ARC_NAMESPACE, None)
        self.nsgml = self.root.newNs(GML_NAMESPACE, "gml")
        self.doc.setNs(self.nsarc)
        self.doc.setNs(self.nsgml)

    def read(self, fd):
        """
        Read the Metadata information from the given file descriptor
        """
        header = self.Header()
        if not header.read(fd):
            return False
        str = fd.read(header.size)

        self.doc = libxml2.parseDoc(str)
        self.root = self.doc.getRootElement()

        for d in self.root.nsDefs():
            if d.content == ARC_NAMESPACE:
                self.nsarc = d
            elif d.content == GML_NAMESPACE:
                self.nsgml = d
            
        return True

    def write(self, fd, formatted = True):
        """
        Write the Metadata information to the given file descriptor
        """
        s = StringIO()
        buf = libxml2.createOutputBuffer(s, "UTF-8")
        buf.saveFormatFileTo(self.doc, "UTF-8", formatted)
        header = self.Header()
        header.size = s.len
        header.write(fd)
        fd.write(s.getvalue())

    def get_origins(self):
        """
        Return the origin information, as list of tuples.

        The first element of the tuple is the style of the origin information;
        the rest is the origin information.
        """
        res = []

        for el in _iter_elements(self.root, self.nsarc, "origin"):
            style = el.prop("style")
            if style == "GRIB":
                # Get centre, subcentre and process
                centre = None
                for el1 in _iter_elements(el, self.nsarc, "centre"):
                    centre = int(_text_contents(el1))
                    break;
                subcentre = None
                for el1 in _iter_elements(el, self.nsarc, "subcentre"):
                    subcentre = int(_text_contents(el1))
                    break;
                process = None
                for el1 in _iter_elements(el, self.nsarc, "process"):
                    process = int(_text_contents(el1))
                    break;
                res.append( (style, centre, subcentre, process) )

        return res 

    def set_origins(self, origins):
        """
        Set the origin information, from a sequence of tuples.

        The first element of the tuple is the style of the origin information;
        the rest is the origin information.
        """
        # Remove all existing reftime elements
        for el in _iter_elements(self.root, self.nsarc, "origin"):
            el.unlinkNode()
            el.freeNode()

        # Add the new elements
        for o in origins:
            if o[0] == "GRIB":
                origin = self.root.newChild(self.nsarc, "origin", None)
                origin.newNsProp(self.nsarc, "style", o[0])
                if o[1] != None:
                    origin.newChild(self.nsarc, "centre", str(o[1]))
                if o[2] != None:
                    origin.newChild(self.nsarc, "subcentre", str(o[2]))
                if o[3] != None:
                    origin.newChild(self.nsarc, "process", str(o[3]))

    def get_reference_time_info(self):
        """
        Return all reference time information from the metadata object.

        The result is a list that contains:
         * datetime objects for instant reference times
         * 2-tuples of datetime objects for ranges of reference times
        """
        res = []

        reftime = None
        for el in _iter_elements(self.root, self.nsarc, "reftime"):
            reftime = el
            break

        if reftime == None:
            return res

        for el in _iter_elements(reftime, self.nsgml, "timePosition"):
            res.append(_datetime_contents(el))

        for el in _iter_elements(reftime, self.nsgml, "timePeriod"):
            start = None
            end = None

            # Now find beginPosition and endPosition, then get them
            for el1 in _iter_elements(el, self.nsgml, "beginPosition"):
                start = _datetime_contents(el1)
                break
            for el1 in _iter_elements(el, self.nsgml, "endPosition"):
                end = _datetime_contents(el1)
                break

            if start != None and end != None:
                res.append( (start, end) )

        return res

    def set_reference_time_info(self, time):
        """
        Set the reference time for this metadata to the value in time.

        Time can be:
         * a datetime object for an instant reference time
         * a 2-tuple of datetime objects for a range of reference times
        """
        # Remove all existing reftime elements
        for el in _iter_elements(self.root, self.nsarc, "reftime"):
            el.unlinkNode()
            el.freeNode()
        # Create a new reftime element
        reftime = self.root.newChild(self.nsarc, "reftime", None)

        if isinstance(time, datetime):
            # Instant reference time
            reftime.newChild(self.nsgml, "timePosition", _to_iso8601(time))
        else:
            # Interval reference time
            tp = reftime.newChild(self.nsgml, "timePeriod", None)
            begin = tp.newChild(self.nsgml, "beginPosition", _to_iso8601(time[0]))
            if abs(datetime.now() - time[1]) < timedelta(days=7):
                end = tp.newChild(self.nsgml, "endPosition", None)
                end.newNsProp(self.nsgml, "indeterminatePosition", "now")
            else:
                end = tp.newChild(self.nsgml, "endPosition", _to_iso8601(time[1]))

    def add_note(self, text):
        """
        Add a note to the document.  All previous notes remain unaltered.
        """
        n = self._obtainElement(self.root, self.nsarc, "notes")
        note = n.newChild(self.nsarc, "note", text)
        note.newNsProp(self.nsarc, "time", _to_iso8601(datetime.now()))

    def get_notes(self):
        """
        Get a list with all the notes in this metadata, and their time.
        """
        notes = None
        for el in _iter_elements(self.root, self.nsarc, "notes"):
            notes = el
            break
        #print >>sys.stderr, "NN", notes
        if notes == None: return []
        res = []
        for el in _iter_elements(notes, self.nsarc, "note"):
            #dt = el.nsProp("time", self.nsarc)
            dt = el.nsProp("time", ARC_NAMESPACE)
            res.append( (_from_iso8601(dt), _text_contents(el)) )
        return res

    def get_dataset(self):
        """
        Get the dataset name and ID for this metadata
        """
        el_ds = _get_element(self.root, self.nsarc, "dataset")
        if el_ds == None:
            return None, None

        el_name = _get_element(el_ds, self.nsarc, "name")
        if el_name == None:
            return None, None

        el_id = _get_element(el_ds, self.nsarc, "id")
        if el_id == None:
            return el_name.content, None
        else:
            return el_name.content, el_id.content

    def set_dataset(self, name, id=None):
        """
        Assign this metadata to the dataset with the given name, using the given ID.
        
        ID can be None, to mean a metadata assigned to a dataset but not imported yet.
        """
        el_ds = self._obtainElement(self.root, self.nsarc, "dataset")
        el_ds.setNsProp(self.nsarc, "changed", _to_iso8601(datetime.now()))

        el_name = self._obtainElement(el_ds, self.nsarc, "name")
        if name != None:
            el_name.setContent(name)
        else:
            el_name.unlinkNode()
            el_name.freeNode()

        el_id = self._obtainElement(el_ds, self.nsarc, "id")
        if id != None:
            el_id.setContent(id)
        else:
            el_id.unlinkNode()
            el_id.freeNode()

if __name__ == "__main__":
    import sys, unittest

    verbose = ("--verbose" in sys.argv)
    #print >>sys.stderr, verbose

    class Test(unittest.TestCase):
        def setUp(self):
            self.md = Metadata()
            self.md.create()

        def testOrigins(self):
            origins = [
                ( "GRIB", 1, 2, 3 ),
                ( "GRIB", 4, 3, 2 ),
                ( "GRIB", 6, 3, 1 ) ]
            self.md.set_origins(origins)
            origins1 = self.md.get_origins()
            self.assertEqual(origins, origins1)
            if verbose: self.md.write(sys.stderr)

        def testReferenceTimeInfo(self):
            time = datetime(2002, 9, 8, 7, 6, 5)
            self.md.set_reference_time_info(time)
            time1 = self.md.get_reference_time_info()
            self.assertEqual([time], time1)

            timea = datetime(2002, 6, 5, 4, 3, 2)
            timeb = datetime(2002, 7, 6, 5, 4, 3)
            self.md.set_reference_time_info((timea, timeb))
            if verbose: self.md.write(sys.stderr)
            times = self.md.get_reference_time_info()
            self.assertEqual(len(times), 1)
            timea1, timeb1 = times[0]

            self.assertEqual(timea, timea1)
            self.assertEqual(timeb, timeb1)

        def testNotes(self):
            self.md.add_note("this is a test note")
            self.md.add_note("this is another test note")
            notes = self.md.get_notes()
            self.assertEqual(len(notes), 2)
            self.assert_( (datetime.now() - notes[0][0]) < timedelta(hours = 1))
            self.assertEqual(notes[0][1], "this is a test note")
            self.assert_( (datetime.now() - notes[1][0]) < timedelta(hours = 1))
            self.assertEqual(notes[1][1], "this is another test note")
            if verbose: self.md.write(sys.stderr)

        def testDataset(self):
            a, b = self.md.get_dataset()
            self.assertEqual(a, None)
            self.assertEqual(b, None)

            self.md.set_dataset("one", "two")
            a, b = self.md.get_dataset()
            self.assertEqual(a, "one")
            self.assertEqual(b, "two")

            self.md.set_dataset("one", None)
            a, b = self.md.get_dataset()
            self.assertEqual(a, "one")
            self.assertEqual(b, None)

            self.md.set_dataset(None, None)
            a, b = self.md.get_dataset()
            self.assertEqual(a, None)
            self.assertEqual(b, None)

    #import sys
    #md = Metadata()
    #md.create()
    #md.root.appendChild(md.doc.createElement("gml:antani"))
    #md.root.appendChild(md.doc.createElementNS("http://www.opengis.net/gml", "gml:blinda"))
    #md.write(sys.stdout)

    unittest.main()
