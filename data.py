import escode

import contextlib import contextmanager
from collections import defaultdict

from lib.datastore.config import DATABASE_NAME
from lib.datastore.datametaclass import DataMetaClass
from lib.datastore.datastore import DataStore
from lib.datastore.events import DataEvents

DATASTORE = DataStore.getInstance(DATABASE_NAME)

class Data(object):

    __metaclass__ = DataMetaClass

    # cache of instances per (edgetype, gid1, gid2)
    _instanceCache = {}

    #
    # caches queries made so far so we can reuse them
    # colo -> (edgetype, gid1) -> (index|gid2) -> result
    #
    _queryCache = defaultdict(lambda: defaultdict(dict))
    _queryCacheDisabled = False

    # instances fetched under lock
    _lockedInstances = set()

    # instances that are dirtied or added under a lock
    _dirtyInstances = set()

    # instances that are deleted under a lock
    _deleteInstances = set()

    # colos that are currently locked
    _lockedColos = set()

    # list of encoders
    _encoders = [escode]
    _currentEncodingIndex = 0

    @property
    def __gid1__(self):
        return getattr(self, self.__gid1attrname__)

    @property
    def __gid2__(self):
        return getattr(self, self.__gid2attrname__)

    @property
    def __cologid__(self):
        return getattr(self, self.__cologidattrname__)

    @classmethod
    def generateGid(cls, colo_gid=None, colo=None):
        return DATASTORE.generateGid(colo_gid, colo)

    @classmethod
    def add(cls, gid1, gid2, attrsdict={}, get=False):
        assert gid1 and gid2, "parent_gid(%d) or child_gid(%d) missing" % (gid1, gid2)
        colo = cls.checkLock(gid1, required=True)
        instance = cls.get(gid1, gid2)
        assert not instance or get, "duplicate instance (%s,%s,%s)" % (cls, gid1, gid2)

        if not instance:
            instance = cls(gid1, gid2, attrsdict)
            instance._markDirty()

        return instance

    @classmethod
    def delete(cls, gid1, gid2):
        assert gid1 and gid2, "parent_gid(%d) or child_gid(%d) missing" % (gid1, gid2)
        colo = cls.checkLock(gid1, required=True)
        instance = cls.get(gid1, gid2)
        instance and instance._markDelete()

    @classmethod
    def query(cls, *args, **kwargs):
        return Query(cls, *args, **kwargs)

    @classmethod
    def queryfetch(cls, query):
        assert query.args, "cannot query without args"
        gid1 = first(arg.value for arg in query.args if arg.attrdef.name == cls.__gid1attrname__)
        key = first(arg.value for arg in query.args if arg.attrdef.name == cls.__keyattrname__)
        colo = key and cls.key2colo(key) or None

        assert colo or gid1 or not Data.insideLock(), "non parent/key query inside lock forbidden"
        colo = cls.checkLock(gid1, colo=colo)

        index = None
        if not gid1 or len(query.args) > 1:
            index = self._getQueryIndex(query)

        # check cache
        cached = cls._getQueryCache(gid1, index)
        if cached: return list(cached)

        # fetch list
        edgedatas = DATASTORE.query(cls.__edgetype__, index=index, gid1=gid1, colo=colo)
        instances = [cls._getInstanceFromEdge(edgedata) for edgedata in edgedatas]

        # update cache
        cls._setQueryCache(gid1, index, instances)
        for instance in instances:
            cls._setQueryCache(gid1, instance.__gid2__, instance)

        return instances

    @classmethod
    def get(cls, gid1, gid2):
        assert gid1 and gid2, "parent_gid(%d) or child_gid(%d) missing" % (gid1, gid2)
        colo = cls.checkLock(gid1)

        # check cache
        cached = cls._getQueryCache(gid1, gid2)
        if cached: return cached

        # get instance
        edgedata = DATASTORE.get(cls.__edgetype__, gid1, gid2)
        instance = cls._getInstanceFromEdge(edgedata) if edgedata else None

        # update cache
        cls._setQueryCache(gid1, gid2, instance)

        return instance

    @classmethod
    def count(cls, gid1):
        assert gid1, "parent_gid(%d) missing" % gid1
        colo = cls.checkLock(gid1)

        # check cache
        cached = cls._getQueryCache(gid1, '__count__')
        if cached: return cached

        # get count
        count = DATASTORE.count(cls.__edgetype__, gid1)

        # update cache
        cls._setQueryCache(gid1, '__count__', count)

        return count

    def refresh(self):
        if not self.__locked__:
            self.get(self.__gid1__, self.__gid2__)
        return self

    def remove(self):
        return self.delete(self.__gid1__, self.__gid2__)

    def populate(self, attrsdict):
        for attr_name, attr_value in attrsdict.iteritems():
            setattr(self, attr_name, attr_value)

    def dict(self, validate=False):
        attrsdict = {}
        for attrname, attrdef in self.__attrdefs__.iteritems():
            attrvalue = getattr(self, attrname)
            if attrvalue is not None:
                attrsdict[attrname] = attrdef._to_base_type(attrvalue)
            elif validate and attrdef.required:
                assert 0, "attr {} is required".format(attrname)
        return attrsdict

    def __setattr__(self, attr_name, attr_value):
        # cannot assign gid1 or gid2
        assert attr_name not in (self.__gid1attrname__, self.__gid2attrname__)

        attr_def = self.__attrdefs__.get(attr_name)
        if attr_def:
            assert self.__locked__, "cannot make changes without a lock "
            self.__datadict__[attr_name] = attr_def.setter(attr_value)
            self._markDirty()
        else:
            super(Data, self).__setattr__(attr_name, attr_value)

    def __getattr__(self, attr_name):
        attr_def = self.__attrdefs__.get(attr_name)
        if attr_def:
            assert not Data.insideLock() or self.__locked__, "using unlocked data inside lock"
            return attr_def.getter(self.__datadict__.get(attr_name))
        else:
            raise AttributeError('%r has no attr `%s`' %  (self.__class__, attr_name))

    def _save(self):
        assert self.__locked__, "lock data before changes"
        assert self.__dirty__, "unexpected: unchanged instance being saved"

        cls, gid1, gid2 = self.__class__, self.__gid1__, self.__gid2__
        colo = cls.colo(gid1)

        # confirm that this is in the same colo as the colo gid
        if cls.__cologidattrname__:
            colo_gid = getattr(self, cls.__cologidattrname__)
            assert colo_gid, "missing colo gid"
            assert cls.colo(colo_gid) == colo, "gid does not match colo colo"
        if cls.__keyattrname__:
            key = getattr(self, cls.__keyattrname__)
            assert key, "missing user defined key"
            assert cls.colo(cls.key2colo(key)) == colo, "gid does not match key colo"

        # validate and encode the data
        attrsdict = self.dict(validate=True)
        data = Data._encoders[Data._currentEncodingIndex].encode(attrsdict)
        encoding = Data._currentEncodingIndex
        indices = self._generateIndices()

        # only overwrite data if we already have a previous revision
        overwrite = bool(self.__revision__)

        # add the edge and save whether it was an overwrite
        edgedata = DATASTORE.add(self.__edgetype__, gid1, gid2, encoding, data, indices, overwrite)
        cls.lastAddWasOverwrite = DATASTORE.lastAddWasOverwrite

        # set the updated revision
        order, revision, edgetype, gid1, gid2, encoding, data = edgedata
        self.__revision__ = revision

        # clear all cached queries that might include this instance
        # and add it as the result to the get query cache
        Data._queryCache[colo][(edgetype, gid1)].clear()
        Data._queryCache[colo][(edgetype, gid1)][gid2] = self

    def _generateIndices(self):
        return [indextuple for index in self.__index__ for indextuple in index.tuples(self)]

    def _getQueryIndexRange(cls, query):
        equal_args = {arg.attrdef.name: arg.value for arg in query.args if arg.op == Query.OP_EQ}
        other_args = [arg for arg in query.args if arg.op != Query.OP_EQ]

        # query validation
        other_attrname = {arg.attrdef.name for arg in other_args}
        assert len(other_attrname) <= 1, "more than 1 inequality arg"
        other_attrname = first(other_attrname)

        arg_start = [arg for arg in other_args if arg.op in (Query.LT, Query.LTE)]
        arg_end = [arg for arg in other_args if arg.op in (Query.GT, Query.GTE)]
        assert len(arg_start) <= 1 and len(arg_end) <= 1, "conflicting inequality args"
        arg_start = first(arg_start)
        arg_end = first(arg_end)

        assert (
            not len(query.orderargs) or
            not other_attrname or
            first(query.orderargs).attrdef.name == other_attrname), \
            "inequality arg should be first order arg"

        index = first(
            index for index in self.__index__
            if index.match(equal_args, query.orderargs or other_args))

        assert index, "no matching index"
        return index.range(equal_args, arg_start, arg_end)

    def _delete(self):
        return DATASTORE.delete(self.__edgetype__, self.__gid1__, self.__gid2__)

    def __new__(cls, gid1, gid2, attrsdict=None):
        assert gid1 and gid2, "cannot create instance without a parent or child defined"

        # check if the instance is cached
        instance_key = (cls.__edgetype__, gid1, gid2)
        instance = Data._instanceCache.get(instance_key)

        # create a new instance if one isn't in the instance cache
        if not instance:
            instance = super(Data, cls).__new__(cls, gid1, gid2, attrsdict)
            Data._instanceCache[instance_key] = instance

        # set the instance as locked if accessed under a lock
        if cls.isLocked(gid1):
            instance.__locked__ = True
            cls._lockedInstances[cls.colo(gid1)].add(instance)

        # set instance attributes if necessary
        if attrsdict:
            instance.populate(attrsdict)

        return instance

    def __init__(self, gid1, gid2, attrsdict=None):
        self.__dict__[self.__gid1attrname__] = gid1
        self.__dict__[self.__gid2attrname__] = gid2

        self.__committeddatadict__ = None
        self.__committedrevision__ = 0
        self.__datadict__ = {}
        self.__revision__ = 0
        self.__locked__ = False
        self.__dirty__ = False
        self.__delete__ = False

    @classmethod
    def _getInstanceFromEdge(cls, edgedata):
        order, revision, edgetype, gid1, gid2, encoding, data = edgedata
        instance = cls(gid1, gid2, None)

        # update the instance if we don't have the most recent revision
        if instance.__revision__ < revision:
            encoder = Data._encoders[encoding]
            rawdata = encoder.decode(data)

            datadict = {
                attrname: self.attrdef._from_base_type(rawdata.get(attrname))
                for attrname, attrdef in self.__attrdefs__.iteritems()}

            instance.__datadict__ = instance.__committeddatadict__ = datadict
            instance.__revision__ = instance.__committedrevision__ = revision

        return instance

    @staticmethod
    @contextlib.contextmanager
    def disabledQueryCache():
        # disable query cache
        queryCacheDisabledOld = Data._queryCacheDisabled
        Data._queryCacheDisabled = True

        try:
            yield
        finally:
            # only the outermost with statemnt will set this back to its original
            # value since all others will see a disabled=True as the old value
            Data._queryCacheDisabled = queryCacheDisabledOld

    @classmethod
    def colo(cls, gid):
        return DATASTORE.colo(gid)

    @staticmethod
    def isLocked(colo):
        return colo in Data._lockedColos

    @staticmethod
    def insideLock():
        return Data._lockedColos

    @classmethod
    def checkLock(cls, gid1, required=False, colo=None):
        # ensure we're under a correct lock before accessing instances
        assert not (gid1 and colo), "unexpected: cannot check both gid1 and colo"
        colo = colo or cls.colo(gid1)
        inside_lock = Data.insideLock()
        assert (not required and not inside_lock) or Data.isLocked(colo), "lock required"
        return colo

    @staticmethod
    @contextlib.contextmanager
    def lock(gids=[]):

        colos = set(map(Data.colo, gids))

        # empty lock waits for the first non empty lock
        if not colos:
            yield
            return

        # nested locks are noops
        if Data.insideLock():
            assert colos.issubset(Data._lockedColos), "cannot acquire new locks inside a lock"
            yield
            return

        Data._lockedColos = colos
        dirty_instances = Data._dirtyInstances
        delete_instances = Data._deleteInstances
        locked_instances = Data._lockedInstances

        assert not (dirty_instances or delete_instances or locked_instances)

        # clear the query cache
        for colo in colos:
            Data._clearQueryCache(colo)

        try:

            with contextlib.nested(map(DATASTORE.lock, sorted(Data._lockedColos))):
                # all updates, adds and deletes will be stored in
                # dirty_instances and delete_instances
                yield

                # save any dirty instances
                for instance in dirty_instances:
                    instance._save()

                # save any dirty instances
                for instance in delete_instances:
                    instance._delete()

        except:

            # revert all changes, clear the cache again because add/get/list might
            # have populated it using new and changed data. also re-raise the exception

            for colo in colos:
                Data._clearQueryCache(colo)

            for instance in dirty_instances:
                instance.__datadict__ = instance.__committeddatadict__
                instance.__revision__ = instance.__committedrevision__

            raise

        else:

            # store a copy of the freshly committed data dict and revision
            # this will be used to revert back to a committed state on future
            # changes

            for instance in dirty_instances:
                instance.__committeddatadict__ = dict(instance.__datadict__)
                instance.__committedrevision__ = instance.__revision__

        finally:

            # none of the instances are any longer locked
            for instance in locked_instances:
                instance.__locked__ = False
            locked_instances.clear()

            # none of the instances are any longer dirty
            for instance in dirty_instances:
                instance.__dirty__ = False
            dirty_instances.clear()

            # none of the instances are any longer deleted
            for instance in delete_instances:
                instance.__delete__ = False
            dirty_instances.clear()

            Data._lockedColos.clear()

    def _markDelete(self):
        if not self.__delete__:
            self.__delete__ = True
            Data._deleteInstances.add(self)

    def _markDirty(self):
        if not self.__dirty__:
            self.__dirty__ = True
            Data._dirtyInstances.add(self)

    @staticmethod
    def clearInstanceCache():
        Data._instanceCache.clear()

    @classmethod
    def _getQueryCache(cls, gid1, query):
        cache = Data._queryCache[cls.colo(gid1)][(cls.__edgetype__, gid1)]
        if not Data._queryCacheDisabled and query in cache:
            return cache[query]

    @classmethod
    def _setQueryCache(cls, gid1, query, value):
        Data._queryCache[cls.colo(gid1)][(cls.__edgetype__, gid1)][query] = value

    @classmethod
    def _clearQueryCache(cls, colo, gid1=None):
        cache = Data._queryCache[colo]
        if gid1:
            cache = cache[(cls.__edgetype__, gid1)]
        cache.clear()

    @staticmethod
    def clearQueryCache():
        Data._queryCache.clear()

    def debug_print(self):
        gid1name = self.__gid1attrname__
        gid2name = self.__gid2attrname__

        padding = max(len(attrname) for attrname in self.__attrdefs__)
        padding = max(padding, len(gid1name), len(gid2name), 10)

        format = "    %%%ds: %%s" % padding

        print self.__class__.__name__
        print format % (gid1name, getattr(self, gid1name))
        print format % (gid2name, getattr(self, gid2name))
        for attrname in self.__attrdefs__:
            print format % (attrname, getattr(self, attrname))
        print format % ('_revision', self.__revision__)
