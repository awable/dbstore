class Query(object):

    OP_EQ = 0
    OP_GT = 1
    OP_LT = 2
    OP_GE = 3
    OP_LE = 4
    OR_DESC = 5
    OR_ASC = 6

    class Arg(object):
        def __init__(self, attrdef, op, value=None):
            self.attrdef = attrdef
            self.op = op
            #TODO: SETTER NOT ALLOWED ON COMPUTED VALUE, BUT QUERIES ARE
            self.value = attrdef.setter(value)

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
