import copy

from lib.edgestore.config import DATABASE_NAME
from lib.edgestore.edgestore import EdgeStore
from lib.edgestore.attr import Attr

_DEFINITIONS_EDGETYPE = 1
_DEFINITIONS_INDEXTYPE = 1
_DEFINITIONS_GID = 1

EDGESTORE = EdgeStore.getInstance(DATABASE_NAME)

class DataMetaClass(type):

    def __new__(cls, name, parents, attrs):
        class_origname = attrs.get('__origname__', name)
        edgetype = DataMetaClass.getDefinitionType(class_origname)
        assert edgetype not in DataMetaClass._dataClasses, "duplicate class `%s`" % class_origname

        cls_parents = filter(lambda parent: isinstance(parent, cls), parents)
        primary_parent = cls_parents[0] if cls_parents else None

        attrsdict = {}
        gid1attrname = primary_parent.__gid1attrname__ if primary_parent else None
        gid2attrname = primary_parent.__gid2attrname__ if primary_parent else None
        cologidattrname = primary_parent.__cologidattrname__ if primary_parent else None
        keyattrname = primary_parent.__keyattrname__ if primary_parent else None

        # add parent attrs to attrs
        for parent in cls_parents:
            for attrname in parent.__attrs__:
                assert attrname not in attrs, "redefined attr `%s`" % attrname
                attrs[attrname] = copy.copy(parent.__attrs__[attrname])

        for attr_name, attr_def in attrs.items():

            if isinstance(attr_def, Attr):

                # remove all attr defs from class
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
                    assert attr_def.name not in attrsdict, 'redefined attr %s' % attr_name
                    attrsdict[attr_def.name] = attr_def

                    if isinstance(attr_def, Attr.ColoGid):
                        assert keyattrname is None, "cannot define key and colo gid"
                        assert cologidattrname is None, "multiple colo gid attrs"
                        cologidattrname = attr_def.name

                    if isinstance(attr_def, Attr.PrimaryKey):
                        assert cologidattrname is None, "cannot define key and colo gid"
                        assert keyattrname is None, "multiple key attrs"
                        keyattrname = attr_def.name

            elif attr_name == '__index__':
                index_def = attr_def
                assert all(attr_def in attrsdict for attr_def in index_def), "invalid index attr"

        attrs['__gid1attrname__'] = gid1attrname
        attrs['__gid2attrname__'] = gid2attrname
        attrs['__edgetype__'] = edgetype
        attrs['__cologidattrname__'] = cologidattrname
        attrs['__keyattrname__'] = keyattrname
        attrs['__attrs__'] = attrsdict

        data_class = super(DataMetaClass, cls).__new__(cls, name, parents, attrs)
        DataMetaClass._dataClasses[edgetype] = data_class
        return data_class

    @staticmethod
    def getDefinitionType(defname):
        with EDGESTORE.lock(EDGESTORE.colo(_DEFINITIONS_GID)):
            edges = EDGESTORE.list(
                _DEFINITIONS_EDGETYPE,
                _DEFINITIONS_GID,
                _DEFINITIONS_INDEXTYPE,
                defname, defname)
            if edges:
                assert len(edges) == 1, "unexpected: got more than one definition for class"
                deftype_gid = edges[0][2]
            else:
                deftype_gid = EDGESTORE.generateGid(colo_gid=_DEFINITIONS_GID)
                EDGESTORE.add(
                    _DEFINITIONS_EDGETYPE,
                    _DEFINITIONS_GID,
                    edgetype_gid,
                    encoding=0,
                    data=defname,
                    indices=[(_DEFINITIONS_INDEXTYPE, defname, True)])

            return deftype_gid << 32

    @staticmethod
    def getDataClass(gid):
        return DataMetaClass._dataClasses.get(gid)
