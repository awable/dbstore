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
            self.value = attrdef.validate(value)

    def __init__(self, cls, *args):
        self._validateargs(args)
        self.datacls = cls
        self.args = args
        self.orderargs = []

    def order(self, *args):
        self._validateargs(args)
        self.orderags.extend(args)

    def filter(*args):
        self._validateargs(args)
        self.args.extend(args)

    def fetch(self):
        return self.datacls.queryfetch(self)

    def _validateargs(args):
        assert all(isinstance(arg, Query.Part) for arg in args), "invalid query arg"
