import escode

from edgestore.query import Query

class Index(object):

    def __init__(self, cls, attrdefs, unique):
        self.cls = cls
        self.attrdefs = attrdefs
        self.attrnames = [attrdef.name for attrdef in attrdefs]
        self.unique = unique

    def tuple(self, attrs):
        value = escode.encode_index(tuple(map(attrs.get, self.attrnames)))
        return (self.indextype, value, self.unique)

    def range(self, equal_args, range_start=None, range_end=None):
        num_equal_attrs = len(equal_attrs)
        index = [equal_args[self.attrnames[idx]] for idx in range(num_equal_attrs)]
        indexstart = indexend = None

        if range_start:
            attrdef, operation, value = range_start
            indexstart = escode.encode_index(tuple(indexvalue + [value]), operation==Query.OP_GE)

        if range_end:
            attrdef, operation, value = range_start
            indexend = escode.encode_index(tuple(indexvalue + [value]), operation==Query.OP_LE)

        if not (range_start or range_end):
            indexstart = indexend = escode.encode_index(tuple(index))

        return (indexstart, indexend)

    def match(self, equal_attrs, other_attrs):
        num_equal_attrs = len(equal_attrs)
        num_other_attrs = len(other_attrs)

        if num_equal_attrs + num_other_attrs > len(self.attrnames):
            return False

        # make sure this index is fully prefixed by equal attrs
        if any(self.attrnames[idx] not in equal_attrs
               for idx in range(num_equal_attrs)):
            return False

        # make sure this index has other args in the correct order
        if any(self.attrnames[num_equal_attrs + idx] != attrname
               for idx, attrname in enumerate(other_attrs)):
            return False

        return True
