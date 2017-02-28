import copy

from attr import Attr

class DataType(type):

    def __init__(self, name, parents, attrs):
        super(DataType, self).__init__(name, parents, attrs)

        # initialize attrdefs and remove them into a seperate variable
        # to avoid accessing them via instance member variables
        self.__attrdefs__ = attrdefs = {}

        # copy parent attrdefs to attrs
        for parent in parents:
            if isinstance(parent, DataType):
                for attrname in parent.__attrdefs__:
                    assert attrname not in attrdefs, "redefined attr `%s`" % attrname
                    attrdef = copy.deepcopy(parent.__attrdefs__[attrname])
                    attrdefs[attrname] = attrdef

        for attrname, attrdef in self.__dict__.items():
            if isinstance(attrdef, Attr):
                delattr(self, attrname)
                attrdef.setname(attrname)
                assert attrname not in attrdefs, "redefined attr `%s`" % attrname
                attrdefs[attrdef.name] = attrdef

    def __getattr__(self, attrname):
        if attrname in self.__attrdefs__:
            return self.__attrdefs__[attrname]

        raise AttributeError('%s has no attr `%s`' % (self, attrname))

class Data(object):

    __metaclass__ = DataType

    def __init__(self, **attrs):
        self.initialize()
        self.populate(attrs)

    def initialize(self):
        self.__datadict__ = {}

    def populate(self, attrsdict):
        for attrname, attrvalue in attrsdict.iteritems():
            setattr(self, attrname, attrvalue)

    def __setattr__(self, attrname, attrvalue):
        attrdef = self.__attrdefs__.get(attrname)
        if attrdef:
            attrdef.set(self, attrvalue)
        else:
            super(Data, self).__setattr__(attrname, attrvalue)

    def __getattr__(self, attrname):
        attrdef = self.__attrdefs__.get(attrname)
        if attrdef:
            return attrdef.get(self)

        raise AttributeError('%s has no attr `%s`' % (self, attrname))

    def dict(self, validate=False):
        attrsdict = {}
        for attrname, attrdef in self.__attrdefs__.iteritems():
            attrvalue = attrdef.get(self)
            if attrvalue is not None:
                attrsdict[attrname] = attrdef._to_base_type(attrvalue)
            elif validate and attrdef.required:
                assert 0, "attr `{}` is required".format(attrname)
        return attrsdict

    def debug_print(self, prefix=''):
        import pprint
        pprint.pprint(self.dict())
