import time
import siteinit

from lib.datastore.entity import Entity
from lib.datastore.attr import Attr
from lib.datastore.data import Data

class TestUserEntity(Entity):
    email = Attr.Unicode(required=True)
    password = Attr.Unicode(required=True)
    dobtime = Attr.Int(required=True)
    jointime = Attr.Int(required=True)
    counter = Attr.Int(required=True, default=0)


def add(gid=None, email='awable@gmail.com'):
    return TestUserEntity.add(gid=gid, attrs=dict(
            email=email,
            password='b4llz',
            dobtime=1344,
            jointime=2342))

with Data.lock(Data.lock_key(SITE_GID)):
    u = TestUserEntity.get(447359172166549504)
    print u
    u.remove()
    print TestUserEntity.get(447359172166549504)
    #add(447359172166549504, email='akhil@relisted.com')
    add(447359172166549504)
    print u
    u.debug_print()
