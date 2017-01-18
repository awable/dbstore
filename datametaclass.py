import copy

from datastore import DataStore
from attr import Attr

_DEFINITIONS_EDGETYPE = 1
_DEFINITIONS_INDEXTYPE = 1
_DEFINITIONS_GID = 1

DATASTORE = DataStore.getInstance()

class DataMetaClass(type):

    _dataClasses = {}
    _disallowedAttrNames = ['get', 'gid1', 'gid2']

    def __new__(cls, name, parents, attrs):
        class_origname = attrs.get('__origname__', name)
        edgetype = DATASTORE.addOrGetDefinitionType(class_origname)
        assert edgetype not in DataMetaClass._dataClasses, "duplicate class `%s`" % class_origname

        cls_parents = filter(lambda parent: isinstance(parent, cls), parents)
        primary_parent = cls_parents[0] if cls_parents else None

        attrdefsdict = {}
        gid1attrname = primary_parent.__gid1attrname__ if primary_parent else None
        gid2attrname = primary_parent.__gid2attrname__ if primary_parent else None
        cologidattrname = primary_parent.__cologidattrname__ if primary_parent else None
        keyattrname = primary_parent.__keyattrname__ if primary_parent else None

        # add parent attrs to attrs
        for parent in cls_parents:
            for attrname in parent.__attrdefs__:
                assert attrname not in attrs, "redefined attr `%s`" % attrname
                attrs[attrname] = copy.copy(parent.__attrdefs__[attrname])

        for attr_name, attr_def in attrs.items():

            if isinstance(attr_def, Attr):

                assert attr_name not in DataMetaClass._disallowedAttrNames, "disallowed attr name"

                # remove class attribute to avoid conflict with instance
                del attrs[attr_name]

                if isinstance(attr_def, Attr.ParentGid):
                    assert gid1attrname is None, "redefined parent gid"
                    gid1attrname = attr_name
                elif isinstance(attr_def, Attr.ChildGid):
                    assert gid2attrname is None, "redefined child gid"
                    gid2attrname = attr_name
                elif isinstance(attr_def, Attr.PrimaryGid):
                    assert gid1attrname is None, "redefined parent gid"
                    assert gid2attrname is None, "redefined child gid"
                    gid1attrname = attr_name
                    gid2attrname = attr_name
                else:
                    attr_def.name = attr_name
                    assert attr_def.name not in attrdefsdict, 'redefined attr %s' % attr_name
                    attrdefsdict[attr_def.name] = attr_def

                    if isinstance(attr_def, Attr.ColoGid):
                        assert keyattrname is None, "cannot define key and colo gid"
                        assert cologidattrname is None, "multiple colo gid attrs"
                        cologidattrname = attr_def.name

                    if isinstance(attr_def, Attr.PrimaryKey):
                        assert cologidattrname is None, "cannot define key and colo gid"
                        assert keyattrname is None, "multiple key attrs"
                        keyattrname = attr_def.name

            elif attr_name == '__index__':
                for index in attrdef:
                    assert all(adef in attrdefsdict for adef in index.attrdefs), "invalid index def"
                    indexname = '{}:{}'.format(name, ':'.join(adef.name for adef in index.attrdefs))
                    index.indextype = DATASTORE.addOrGetDefinitionType(indexname)


        attrs['__gid1attrname__'] = gid1attrname
        attrs['__gid2attrname__'] = gid2attrname
        attrs['__edgetype__'] = edgetype
        attrs['__cologidattrname__'] = cologidattrname
        attrs['__keyattrname__'] = keyattrname
        attrs['__attrdefs__'] = attrdefsdict
        attrs['__index__'] = attrs.get('__index__', [])

        data_class = super(DataMetaClass, cls).__new__(cls, name, parents, attrs)
        DataMetaClass._dataClasses[edgetype] = data_class
        return data_class

    def __getattr__(self, attr_name):
        if attr_name in self.__attrdefs__:
            return self.__attrdefs__[attr_name]

        raise AttributeError('%r has no attr `%s`' %  (self.__class__, attr_name))

    @staticmethod
    def getDataClass(gid):
        return DataMetaClass._dataClasses.get(gid)
