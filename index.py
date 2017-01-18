import escode

from itertools import product
from edgestore.query import Query

class Index(object):

    def __init__(self, attrdefs, unique):
        self.indextype = None # filled in by the metaclass
        self.attrdefs = {attrdef.name: attrdef for attrdef in attrdefs}
        self.attrnames = [attrdef.name for attrdef in attrdefs]
        self.unique = unique
        self.repeated = any(isinstance(attrdef, Attr.Repeated) for attrdef in attrdefs)

    def tuples(self, data_instance):
        attrtuples = [tuple(
            attrdef._to_base_type(getattr(data_instance, attrdef.name))
            for attrdef in self.attrdefs)]

        if self.repeated:
            attrs = [attr if isinstance(attr, list) else [attr] for attr in firts(attrtuples)]
            attrtuples = product(*attrs)

        for attrtuple in attrtuples:
            yield (self.indextype, escode.encode_index(attrtuple), self.unique)


    def range(self, equal_args, arg_start=None, arg_end=None):
        num_equal_args = len(equal_args)
        index = [equal_args[self.attrnames[idx]] for idx in range(num_equal_args)]
        indexstart = indexend = None

        if arg_start:
            attrdef, operation, value = arg_start
            indexstart = escode.encode_index(tuple(indexvalue + [value]), operation==Query.OP_GE)

        if arg_end:
            attrdef, operation, value = arg_start
            indexend = escode.encode_index(tuple(indexvalue + [value]), operation==Query.OP_LE)

        if not (arg_start or arg_end):
            indexstart = indexend = escode.encode_index(tuple(index))

        return (self.indextype, indexstart, indexend)

    def match(self, equal_args, other_args):
        num_equal_args = len(equal_args)
        num_other_args = len(other_args)

        if num_equal_args + num_other_args > len(self.attrnames):
            return False

        # make sure this index is fully prefixed by equal args
        if any(self.attrnames[idx] not in equal_args
               for idx in range(num_equal_args)):
            return False

        # make sure this index has other args in the correct order
        if any(self.attrnames[num_equal_args + idx] != arg.attrdef.name
               for idx, arg in enumerate(other_args)):
            return False

        return True
