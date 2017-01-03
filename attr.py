import copy

from edgestore.gid import Gid
from edgestore.query import Query

class Attr(object):

    ALLOW_REQUIRED = True
    ALLOW_DEFAULT = True

    def __init__(self, required=False, default=None):
        clsname = self.__class__.__name__
        assert self.ALLOW_REQUIRED or not required, "{} cannot have required".format(clsname)
        assert self.ALLOW_DEFAULT or not default, "{} cannot have default".format(clsname)
        assert not (required and default), "required attr cannot have default"

        self.name = None
        self.required = required
        self.default = self.setter(default)

    def getter(self, value):
        return value if value is not None else self.default

    def setter(self, value):
        return self._dovalidate(value)

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

    def _dovalidate(self, value):
        assert not self.required or value is not None, "required `%s` attr missing" % self.name
        if value is not None:
            updated_value = self._validate(value)
            if updated_value is not None:
                value = updated_value
        return value

    def _validate(self, value):
        raise Exception("Cannot instantiate Attr() directly")

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
    value if type(value) is vtype else vtype(value)

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

class AttrList(Attr):
    def _validate(self, value):
        return _castif(list, value)

class AttrDict(Attr):
    def _validate(self, value):
        return _castif(dict, value)

class AttrDateTime(Attr):

    _EPOCH = datetime.datetime.utcfromtimestamp(0)

    def _validate(self, value):
        assert isinstance(value, datetime.datetime) and value.tzinfo, "Require UTC datetime object"

    def _to_base_type(self, value):
        delta = value - _EPOCH
        return delta.microseconds + 1000000 * (delta.seconds + 24 * 3600 * delta.days)

    def _from_base_type(self, value):
        if value is None: return None
        return _EPOCH + datetime.timedelta(microseconds=value)

class AttrGid(AttrInt):
    def __init__(self, *args, **kwargs):
        self.datacls = kwargs.get('cls')
        if datacls: del kwargs[cls]
        super(AttrGid, self).__init__(*args, **kwargs)

    def _validate(self, value):
        if type(value) in (int, long):
            return Gid(self.datacls, value)
        assert isinstance(value, Gid) and self.datacls is value.datacls, "Require int or Gid"

    def _to_base_type(self, value):
        return value.gid

    def _from_base_type(self, value):
        if value is None: return None
        return Gid(self.datacls, value)

class _AttrRequiredGid(AttrGid):
    def __init__(self, *args, **kwargs):
        kwargs['required'] = True
        super(_AttrRequiredGid, self).__init__(*args, **kwargs)

class AttrParentGid(_AttrRequiredGid):
    pass

class AttrChildGid(_AttrRequiredGid):
    pass

class AttrPrimaryGid(_AttrRequiredGid):
    pass

class AttrColoGid(_AttrRequiredGid):
    pass

class AttrPrimaryKey(AttrString):
    pass

Attr.Bool = AttrBool
Attr.Int = AttrInt
Attr.Float = AttrFloat
Attr.String = AttrString
Attr.Unicode = AttrUnicode
Attr.List = AttrList
Attr.Dict = AttrDict
Attr.Gid = AttrGid
Attr.ParentGid = AttrParentGid
Attr.ChildGid = AttrChildGid
Attr.PrimaryGid = AttrPrimaryGid
Attr.ColoGid = AttrColoGid
Attr.PrimaryKey = AttrPrimaryKey
