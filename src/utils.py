from cStringIO import StringIO
import codecs
import csv


def encode_string(v):
    """Helper function that return an utf-8 encoded only if the
    value is a string"""
    if isinstance(v, basestring):
        return v.encode("utf-8")
    return v


class UnicodeDictWriter(object):
    """Provide a DictWriter that deals with Unicode"""

    def __init__(
            self, f, fieldnames, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = StringIO()
        self.writer = csv.DictWriter(self.queue,
                                     fieldnames,
                                     dialect=dialect,
                                     **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow({k: encode_string(v) for k, v in row.items()})
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

    def writeheader(self):
        self.writer.writeheader()


class CsvDictsAdapter(object):
    """Provide a DataChange generator and it provides a file-like
    object which returns csv data"""
    def __init__(self, source_generator):
        self.source = source_generator
        self.buffer = StringIO()
        self.csv = None
        self.add_header = False

    def __iter__(self):
        return self

    def write_header(self):
        self.add_header = True

    def next(self):
        row = self.source.next()

        self.buffer.truncate(0)
        self.buffer.seek(0)

        if not self.csv:
            self.csv = UnicodeDictWriter(self.buffer, row.keys(),
                                         quoting=csv.QUOTE_NONNUMERIC)
            self.csv.writeheader()
        elif self.add_header:
            self.csv.writeheader()
            self.add_header = False

        self.csv.writerow(row)
        self.buffer.seek(0)
        return self.buffer.read()
