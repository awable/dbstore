import time
import datetime
from pprint import pprint

from . import Data, Entity, KeyEntity, EdgeData, Attr, Index

class Phone(Data):
    code = IntAttr(required=True)
    number = IntAttr(required=True)

class TestUserEntity(Entity):
    email = UnicodeAttr(required=True)
    password = UnicodeAttr(required=True)
    dobtime = DateTimeAttr(required=True)
    jointime = IntAttr(required=True)
    counter = IntAttr(default=0)
    phone = RepeatedAttr(LocalDataAttr(Phone), required=True)

    __indexdefs__ = [
        Index(phone.code)]

class TestUserEmailEntity(KeyEntity):
    email = PrimaryKeyAttr()
    name = UnicodeAttr(required=True)

class UserEventAssoc(EdgeData):
    usergid = LocalGidAttr()
    eventgid = RemoteGidAttr()
    subscribed = BoolAttr(default=False)

    __indexdefs__ = [
        Index(usergid, subscribed),
        Index(subscribed)]


def add(gid=None, email='awable@gmail.com'):
    # return TestUserEntity.add(
    #     email=email,
    #     password='b4llzz',
    #     dobtime=datetime.datetime(year=1983,month=12,day=17),
    #     jointime=23422323,
    #     phone=[Phone(code=1, number=2)])

    return TestUserEmailEntity.addbykey(email, name='Akhil')

# pprint(TestUserEntity.__attrdefs__)

# u = TestUserEntity.get(11912401664461504513)

# print TestUserEntity.phone.code.get(u)

u = add()
pprint(u.dict())
