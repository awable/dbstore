from data import Data
from attr import Attr
from datametaclass import DataMetaClass

class Entity(Data):

    gid = Attr.PrimaryGid()

    @classmethod
    def getColoGid(cls, attrs):
        colo_gid = None
        if cls.__cologidattrname__:
            colo_gid = attrs.get(cls.__cologidattrname__)
            assert colo_gid, "missing colo gid"
        return colo_gid

    @classmethod
    def add(cls, gid=None, **attrs):
        gid = gid or Data.generateGid(colo_gid=cls.getColoGid(attrs))
        with Data.lock(gid):
            return super(Entity, cls).add(gid, gid, **attrs)

    @classmethod
    def delete(cls, gid):
        return super(Entity, cls).delete(gid, gid)

    def remove(self):
        return self.delete(self.gid)
