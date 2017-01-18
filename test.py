import time
import datetime

from entity import Entity
from attr import Attr
from data import Data

class TestUserEntity(Entity):
    email = Attr.Unicode(required=True)
    password = Attr.Unicode(required=True)
    dobtime = Attr.DateTime(required=True)
    jointime = Attr.Int(required=True)
    counter = Attr.Int(default=0)

def add(gid=None, email='awable@gmail.com'):
    return TestUserEntity.add(
        email=email,
        password='b4llz',
        dobtime=datetime.datetime(year=1983,month=12,day=14),
        jointime=2342)

#add()

e = TestUserEntity.get(15647085412931862529, 15647085412931862529)
print e
if e: e.debug_print()
e = TestUserEntity.get(15647085412931862529, 15647085412931862529)

