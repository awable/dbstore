import copy
import datetime

from query import Query

class Attr(object):

    ALLOW_REQUIRED = True
    ALLOW_DEFAULT = True

    ALWAYS_REQUIRED = False

    def __init__(self, required=False, default=None):
        clsname = self.__class__.__name__
        assert self.ALLOW_REQUIRED or not required, "{} cannot have required".format(clsname)
        assert self.ALLOW_DEFAULT or not default, "{} cannot have default".format(clsname)
        assert not required or default is None, "required attr cannot have default"

        self.name = None # filled in by DataMetaClass
        self.required = self.ALWAYS_REQUIRED or required
        self.default = self.validate(default) if default is not None else default

    def getter(self, value):
        return value if value is not None else copy.copy(self.default)

    def setter(self, value):
        return self.validate(value)

    def __eq__(self, value):
        return Query.Part(self, Query.OP_EQ, value)

    def __gt__(self, value):
        return Query.Part(self, Query.OP_GT, value)

    def __lt__(self, value):
        return Query.Part(self, Query.OP_LT, value)

    def __ge__(self, value):
        return Query.Part(self, Query.OP_GE, value)

    def __le__(self, value):
        return Query.Part(self, Query.OP_LE, value)

    def __neg__(self):
        return Query.Part(self, Query.OR_DESC)

    def __pos__(self):
        return Query.Part(self, Query.OR_ASC)

    def validate(self, value):
        assert not self.required or value is not None, "required `%s` cannot be None" % self.name
        if value is not None:
            value = self._validate(value)
        return value

    def _validate(self, value):
        return value

    def _to_base_type(self, value):
        return value

    def _from_base_type(self, value):
        return value

class AttrComputed(Attr):

    ALLOW_REQUIRED = False
    ALLOW_DEFAULT = False

    def getter(self, value):
        return value()

    def setter(self, value):
        assert False, "Cannot set AttrComputed"

def _castif(vtype, value):
    return value if type(value) is vtype else vtype(value)

class AttrBool(Attr):
    def _validate(self, value):
        return _castif(bool, value)

class AttrInt(Attr):
    def _validate(self, value):
        return _castif(int, value)

class AttrFloat(Attr):
    def _validate(self, value):
        return _castif(float, value)

class AttrString(Attr):
    def _validate(self, value):
        return _castif(str, value)

class AttrUnicode(Attr):
    def _validate(self, value):
        return _castif(unicode, value)

class AttrRepeated(Attr):
    def __init__(self, attrtype):
        assert issubclass(attrtype, Attr), "need element attr type for AttrRepeated"
        self.elem = attrtype(required=False)
        super(AttrRepeated, self).__init__(required=False, default=[])

    def _validate(self, value):
        return tuple(map(self.elem.validate, value))

    def _from_base_type(self, value):
        return tuple(map(self.elem._from_base_type, value))

    def _to_base_type(self, value):
        return map(self.elem._to_base_type, value)

class AttrDict(Attr):
    def _validate(self, value):
        return _castif(dict, value)

class AttrDateTime(Attr):

    _EPOCH = datetime.datetime.utcfromtimestamp(0)

    def _validate(self, value):
        assert isinstance(value, datetime.datetime) and not value.tzinfo, "Require UTC datetime obj"
        return value

    def _to_base_type(self, value):
        delta = value - self._EPOCH
        return delta.microseconds + 1000000 * (delta.seconds + 24 * 3600 * delta.days)

    def _from_base_type(self, value):
        if value is None: return None
        return self._EPOCH + datetime.timedelta(microseconds=value)

class AttrGid(AttrInt):
    pass

class AttrParentGid(AttrGid):
    ALWAYS_REQUIRED = True

class AttrChildGid(AttrGid):
    ALWAYS_REQUIRED = True

class AttrPrimaryGid(AttrGid):
    ALWAYS_REQUIRED = True

class AttrColoGid(AttrGid):
    ALWAYS_REQUIRED = True

class AttrPrimaryKey(AttrString):
    ALWAYS_REQUIRED = True

Attr.Bool = AttrBool
Attr.Int = AttrInt
Attr.Float = AttrFloat
Attr.String = AttrString
Attr.Unicode = AttrUnicode
Attr.Repeated = AttrRepeated
Attr.DateTime = AttrDateTime
Attr.Dict = AttrDict
Attr.Gid = AttrGid
Attr.ParentGid = AttrParentGid
Attr.ChildGid = AttrChildGid
Attr.PrimaryGid = AttrPrimaryGid
Attr.ColoGid = AttrColoGid
Attr.PrimaryKey = AttrPrimaryKey
