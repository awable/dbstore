import copy
import binascii

from utils import first
from edgedata import EdgeDataType, EdgeData
from attr import Attr
from index import Index

class EntityType(EdgeDataType):

    def __init__(self, name, parents, attrs):
        super(EntityType, self).__init__(name, parents, attrs)

        cls_parents = filter(lambda parent: isinstance(parent, EntityType), parents)
        primary_parent = cls_parents[0] if cls_parents else None
        coloattr = copy.deepcopy(primary_parent.__coloattr__) if primary_parent else None

        for attrdef in self.__attrdefs__.itervalues():
            if isinstance(attrdef, ColoGidAttr):
                assert coloattr is None, "cannot have multiple colo attrs"
                coloattr = attrdef

        self.__coloattr__ = coloattr

class Entity(EdgeData):

    __metaclass__ = EntityType

    gid = LocalGidAttr()
    _gid = RemoteGidAttr()

    @classmethod
    def add(cls, gid=None, **attrs):
        if not gid:
            gid = cls.generateGid(colo_gid=cls._getColoGid(attrs))
            attrs.pop('get', None) # no point in getting if we just created the gid

        with cls.lock(gid):
            return super(Entity, cls).add(gid=gid, _gid=gid, **attrs)

    @classmethod
    def delete(cls, gid, _gid=None):
        return super(Entity, cls).delete(gid, gid)

    def remove(self):
        return self.delete(self.gid)

    @classmethod
    def get(cls, gid, _gid=None):
        return super(Entity, cls).get(gid, gid)

    @property
    def __cologid__(self):
        return getattr(self, self.__coloattr__.name)

    @classmethod
    def _getColoGid(cls, attrs):
        if not cls.__coloattr__:
            return None

        colo_gid = attrs.get(cls.__coloattr__.name)
        assert colo_gid, "missing colo gid"
        return colo_gid

class KeyEntityType(EntityType):
    def __init__(self, name, parents, attrs):
        super(KeyEntityType, self).__init__(name, parents, attrs)

        cls_parents = filter(lambda parent: isinstance(parent, KeyEntityType), parents)
        primary_parent = cls_parents[0] if cls_parents else None
        keyattr = copy.copy(primary_parent.__keyattr__) if primary_parent else None

        for attrdef in self.__attrdefs__.itervalues():
            if isinstance(attrdef, PrimaryKeyAttr):
                assert keyattr is None, "cannot have multiple primary key attrs"
                keyattr = attrdef

        assert keyattr or name is 'KeyEntity', "missing key attr"
        self.__keyattr__ = keyattr

        if keyattr:
            self.addIndex(Index(keyattr, unique=True))

class KeyEntity(Entity):

    __metaclass__ = KeyEntityType

    @classmethod
    def add(cls, gid, key, **attrs):
        assert gid and key, "missing gid or key"
        assert cls.colo(gid) == cls._key2colo(key), "mismatched gid and key"
        attrs[cls.__keyattr__.name] = key
        return super(KeyEntity, cls).add(gid, **attrs)

    @classmethod
    def addbykey(cls, key, **attrs):
        assert key, "cannot use empty key"
        assert not 'gid' in attrs, "cannot include gid in addbykey"

        get = attrs.pop('get', False)

        keyattrname = cls.__keyattr__.name
        assert attrs.get(keyattrname) in (None, key), "conflicting primary key attr"
        attrs[keyattrname] = key

        keycolo = cls._key2colo(key)
        with cls.lock(colos=[keycolo]):
            instance = cls.getbykey(key)
            assert not instance or get, "duplicate instance key(%s)" % key

            if not instance:
                gid = cls.generateGid(colo=keycolo)
                instance = cls.add(gid, key, **attrs)

            return instance

    @classmethod
    def deletebykey(cls, key):
        keycolo = cls._key2colo(key)
        with cls.lock(colos=[keycolo]):
            instance = cls.getbykey(key)
            if instance:
                instance.remove()

    @classmethod
    def getbykey(cls, key):
        keycolo = cls._key2colo(key)
        return first(cls.query(cls.__keyattr__ == key, colo=keycolo).fetch())

    @classmethod
    def _key2colo(cls, key):
        return binascii.crc32(key) & 0xffffffff

    @property
    def __key__(self):
        return getattr(self, self.__keyattr__.name)
