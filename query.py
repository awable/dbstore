import escode

from utils import first

class Query(object):

    OP_EQ = 0
    OP_GE = 1
    OP_GT = 2
    OP_LT = 3
    OP_LE = 4
    OR_DESC = 5
    OR_ASC = 6

    class Arg(object):
        def __init__(self, attrdef, op, value=None):
            self.attrdef = attrdef
            self.op = op
            self.value = None if value is None else attrdef._validate(value)

    def __init__(self, cls, *args, **kwargs):
        self.colo = kwargs.pop('colo', None)
        self.localarg = None
        self.localgid = None
        self.datacls = cls

        self.equalargs = {}

        self.otherargs = []
        self.otherattr = None
        self.argstart = None
        self.argend = None

        self.orderargs = []
        self.orderattrs = []

        self.filter(args)

    @property
    def isindexquery(self):
        numargs = len(self.equalargs) + len(self.otherargs) + len(self.orderargs)
        return numargs > 1 or not self.localarg

    def filter(self, args):
        assert all(isinstance(arg, Query.Arg) for arg in args), "invalid query arg"

        # extract equal args
        eqargs = filter(lambda arg: arg.op == Query.OP_EQ, args)
        assert all(arg.attrdef not in self.equalargs for arg in eqargs), "redefined equality attr"
        self.equalargs.update((arg.attrdef, arg) for arg in eqargs)

        # extract local gids and colo
        self.localarg = self.equalargs.get(self.datacls.__localattr__)
        self.localgid = self.localarg and self.localarg.value
        assert not (self.localgid and self.colo), "conflicting colo arguments"
        self.colo = self.colo or (self.localgid and self.datacls.colo(self.localgid))

        # ensure there is at most one inequality attr
        self.otherargs.extend(filter(lambda arg: arg.op != Query.OP_EQ, args))
        self.otherattrs = {arg.attrdef for arg in self.otherargs}
        assert len(self.otherattrs) <= 1, "more than 1 inequality arg"

        # extract range query from inequality args, and ensure it is valid
        arg_start = [arg for arg in self.otherargs if arg.op in (Query.OP_GT, Query.OP_GE)]
        arg_end = [arg for arg in self.otherargs if arg.op in (Query.OP_LT, Query.OP_LE)]
        assert len(arg_start) <= 1 and len(arg_end) <= 1, "conflicting inequality args"
        self.argstart = first(arg_start)
        self.argend = first(arg_end)

        assert not (self.argstart and self.argend) or self.argstart.value <= self.argend.value, \
            "disjoint inequality attr range"

        assert first(self.orderattrs) in (None, first(self.otherattrs)), \
            "inequality arg should be first order arg"

        return self

    def order(self, *args):
        assert all(isinstance(arg, Query.Arg) for arg in args), "invalid query arg"
        assert all(arg.op in (Query.OR_DESC, Query.OR_ASC) for arg in args), "bad query order op"
        assert all(arg.attrdef not in self.orderattrs for arg in args), "redefined order attr"
        self.orderargs.extend(args)
        self.orderattrs.extend(arg.attrdef for arg in self.orderargs)

        otherattr = first(self.otherattrs)
        assert otherattr is None or otherattr is first(self.orderattrs), \
            "first order arg should be same as first inequality arg"

        return self

    def setcolo(self, colo):
        self.colo = colo
        return self

    def fetch(self):
        return self.datacls.queryfetch(self)

    def range(self, indexdef):
        assert indexdef, "no matching index"

        attrdefs = (indexdef.attrdefs[idx] for idx in range(len(self.equalargs)))
        startvalues = endvalues = tuple(self.equalargs[attrdef].value for attrdef in attrdefs)
        indexstart = indexend = None

        if self.argstart:
            startvalues = startvalues + (self.argstart.value,)

        if self.argend:
            endvalues = endvalues + (self.argend.value,)

        openstart = not self.argstart or (self.argstart.op == Query.OP_GE)
        indexstart = escode.encode_index(startvalues, openstart)

        openend = not self.argend or (self.argend.op == Query.OP_LE)
        indexend = escode.encode_index(endvalues, openend)
        if openend: indexend = indexend + '\x01'

        return (indexdef.indextype, indexstart, indexend)
