import copy

from attr import Attr

class DataType(type):

    def __new__(cls, name, parents, attrs):
        cls_parents = filter(lambda parent: isinstance(parent, cls), parents)

        # copy parent attrdefs to attrs
        for parent in cls_parents:
            for attrname in parent.__attrdefs__:
                assert attrname not in attrs, "redefined attr `%s`" % attrname
                attrs[attrname] = copy.copy(parent.__attrdefs__[attrname])

        # initialize attrdefs and remove them into a seperate variable
        # to avoid accessing them via instance member variables
        attrs['__attrdefs__'] = attrdefs = {}
        for attrname, attrdef in attrs.items():
            if isinstance(attrdef, Attr):
                del attrs[attrname]
                attrdef.name = attrname
                attrdefs[attrdef.name] = attrdef

        return super(DataType, cls).__new__(cls, name, parents, attrs)

    def __getattr__(self, attrname):
        if attrname in self.__attrdefs__:
            return self.__attrdefs__[attrname]

        return super(DataType, self).__getattr__(attrname)
