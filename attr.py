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
        self.default = self._validate(default) if default is not None else default

        # nested attributes
        self.attrdefs = {}

    def setname(self, name):
        self.name = name

    def set(self, instance, attrvalue):
        assert not (self.required and attrvalue is None), "required `%s` cannot be None" % self.name
        attrvalue = None if attrvalue is None else self._validate(attrvalue)
        instance.__datadict__[self.name] = attrvalue

    def get(self, instance):
        attrvalue = instance.__datadict__.get(self.name)
        return attrvalue if attrvalue is not None else copy.deepcopy(self.default)

    def __getattr__(self, attrname):
        if attrname in self.attrdefs:
            return self.attrdefs[attrname]

        raise AttributeError('%s has no attr `%s`' % (self, attrname))

    def __eq__(self, value):
        return Query.Arg(self, Query.OP_EQ, value)

    def __gt__(self, value):
        return Query.Arg(self, Query.OP_GT, value)

    def __lt__(self, value):
        return Query.Arg(self, Query.OP_LT, value)

    def __ge__(self, value):
        return Query.Arg(self, Query.OP_GE, value)

    def __le__(self, value):
        return Query.Arg(self, Query.OP_LE, value)

    def __neg__(self):
        return Query.Arg(self, Query.OR_DESC)

    def __pos__(self):
        return Query.Arg(self, Query.OR_ASC)

    def _validate(self, value):
        return value

    def _to_base_type(self, value):
        return value

    def _from_base_type(self, value):
        return value

class _NestedAttr(Attr):

    def __init__(self, attr, parentattr):
        self.attr = attr
        self.parentattr = parentattr
        self.name = attr.name

    def setname(self, name):
        self.name = name

    def get(self, instance):
        instance = self.parentattr.get(instance)
        if isinstance(instance, tuple):
            return tuple(self.attr.get(inst) for inst in instance)
        return self.attr.get(instance)

    def set(self, instance, attrvalue):
        instance = self.parentattr.get(instance)
        if isinstance(instance, tuple):
            raise AttributeError('%s has no method `set`' % self)
        self.attr.set(instance, attrvalue)

    def __getattr__(self, attrname):
        return getattr(self.attr, attrname)

class ComputedAttr(Attr):

    ALLOW_REQUIRED = False
    ALLOW_DEFAULT = False

    def __init__(self, func):
        super(ComputedAttr, self).__init__()
        self.func = func

    def set(self, instance, attrvalue):
        assert False, "Cannot set ComputedAttr"

    def get(self, instance):
        return self.func(instance)

def _castif(vtype, value):
    return value if type(value) is vtype else vtype(value)

class BoolAttr(Attr):
    def _validate(self, value):
        return _castif(bool, value)

class IntAttr(Attr):
    def _validate(self, value):
        return _castif(int, value)

class FloatAttr(Attr):
    def _validate(self, value):
        return _castif(float, value)

class StringAttr(Attr):
    def _validate(self, value):
        return _castif(str, value)

class UnicodeAttr(Attr):
    def _validate(self, value):
        return _castif(unicode, value)

class RepeatedAttr(Attr):

    ALLOWS_REQUIRED = False

    def __init__(self, elemattr, required=False, **kwargs):
        default = None if required else tuple()
        super(RepeatedAttr, self).__init__(required=required, default=default, **kwargs)

        assert isinstance(elemattr, Attr), "need element type for RepeatedAttr"
        self.elemattr = elemattr
        self.elemattr.name = self.name

        for attrname, attrdef in self.elemattr.attrdefs.iteritems():
            attrtype = type('NestedAttr', (attrdef.__class__,), dict(_NestedAttr.__dict__))
            self.attrdefs[attrname] =  attrtype(attrdef, self)

    def setname(self, name):
        super(RepeatedAttr, self).setname(name)
        self.elemattr.setname(name)

    def _validate(self, value):
        assert isinstance(value, (list, tuple)), "value must be a list/tuple"
        assert not (self.required and not value), "required `%s` cannot be empty" % self.name
        return tuple(map(self.elemattr._validate, value))

    def _from_base_type(self, value):
        return tuple(map(self.elemattr._from_base_type, value))

    def _to_base_type(self, value):
        return map(self.elemattr._to_base_type, value)

    def __getattr__(self, attrname):
        return getattr(self.elemattr, attrname)

class DictAttr(Attr):
    def _validate(self, value):
        return _castif(dict, value)

class DateTimeAttr(Attr):

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

class GidAttr(IntAttr):
    pass

class LocalGidAttr(GidAttr):
    ALWAYS_REQUIRED = True

class RemoteGidAttr(GidAttr):
    ALWAYS_REQUIRED = True

class PrimaryGidAttr(LocalGidAttr, RemoteGidAttr):
    ALWAYS_REQUIRED = True

class ColoGidAttr(GidAttr):
    ALWAYS_REQUIRED = True

class PrimaryKeyAttr(StringAttr):
    ALWAYS_REQUIRED = True

class LocalDataAttr(Attr):

    def __init__(self, nesteddatacls, **kwargs):
        super(LocalDataAttr, self).__init__(**kwargs)
        self.nesteddatacls = nesteddatacls

        for attrname, attrdef in nesteddatacls.__attrdefs__.iteritems():
            attrtype = type('NestedAttr', (attrdef.__class__,), dict(_NestedAttr.__dict__))
            self.attrdefs[attrname] =  attrtype(attrdef, self)

    def setname(self, name):
        super(LocalDataAttr, self).setname(name)
        for attrdef in self.attrdefs.itervalues():
            attrdef.setname('{}.{}'.format(name, attrdef.name))

    def _validate(self, value):
        assert isinstance(value, self.nesteddatacls), "expected %s" % self.nesteddatacls
        return value

    def _from_base_type(self, value):
        return self.nesteddatacls(**value)

    def _to_base_type(self, value):
        return value.dict(validate=True)
