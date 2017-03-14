import time
import datetime
from pprint import pprint

from data import Data
from entity import Entity, KeyEntity
from assoc import Assoc
from attr import *
from index import Index

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

class UserEventAssoc(Assoc):
    usergid = LocalGidAttr()
    eventgid = RemoteGidAttr()
    subscribed = BoolAttr(default=False)

    __indexdefs__ = [
        Index(usergid, subscribed),
        Index(subscribed)]


u = TestUserEmailEntity.add(email='akhilwable@oddbird.org', name='Akhil', get=True)
pprint(u.dict())

with u.locknload():
    u.name += 'Akhil Wable'

pprint(u.dict())
