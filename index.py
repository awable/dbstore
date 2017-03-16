import escode

from collections import Iterable
from itertools import product, chain
from attr import Attr

class Index(object):

    def __init__(self, *attrdefs, **kwargs):
        assert all(isinstance(attrdef, Attr) for attrdef in attrdefs), "invalid attrdef"
        self.indextype = None # filled in by the metaclass
        self.attrdefs = attrdefs
        self.unique = kwargs.pop('unique', False)

    def tuples(self, data_instance):
        attrtuples = [tuple(
            attrdef._to_base_type(attrdef.get(data_instance))
            for attrdef in self.attrdefs)]

        while any(isinstance(attr, Iterable) for attr in attrtuples[0]):
            attrtuples = list(chain.from_iterable((
                product(*[attr if isinstance(attr, Iterable) else [attr] for attr in attrtuple])
                for attrtuple in attrtuples)))

        for attrtuple in attrtuples:
            yield (self.indextype, escode.encode_index(attrtuple), self.unique)

    def match(self, query):
        equalargs = query.equalargs
        otherattrs = query.orderattrs or query.otherattrs
        attrsiter = iter(self.attrdefs)

        return (
            (not self.unique or query.colo) # assures unique indices are restricted to colo
            and (len(equalargs) + len(otherattrs) <= len(self.attrdefs))
            and all(attrsiter.next() in equalargs for idx in range(len(equalargs)))
            and all(attrsiter.next() is attr for attr in otherattrs))
