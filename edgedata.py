import copy
import escode
import contextlib

from collections import defaultdict
from utils import first
from data import DataType, Data
from attr import *
from index import Index
from query import Query
from datastore import DataStore

DATASTORE = DataStore.getInstance()

class EdgeDataType(DataType):

    _edgedataClasses = {}

    # cache of instances per (edgetype, localgid, remotegid)
    _instanceCache = {}

    _RESERVED = {'get'}

    def __init__(self, name, parents, attrs):
        super(EdgeDataType, self).__init__(name, parents, attrs)

        assert all(attrname not in EdgeDataType._RESERVED for attrname in self.__attrdefs__), \
            "reserved keyword attr"

        # get edgetype definition from the datastore
        storename = self.__dict__.get('__storename__', name)
        edgetype = DATASTORE.addOrGetDefinitionType(storename)
        assert not EdgeDataType.getEdgeDataClass(edgetype), "duplicate class `%s`" % storename
        EdgeDataType._edgedataClasses[edgetype] = self

        # extract local and remote attrs from parent if there
        cls_parents = filter(lambda parent: isinstance(parent, EdgeDataType), parents)
        primary_parent = cls_parents[0] if cls_parents else None
        remoteattr = localattr = None

        if primary_parent:
            localattr = copy.deepcopy(primary_parent.__localattr__)
            remoteattr = copy.deepcopy(primary_parent.__remoteattr__)

        for attrdef in self.__attrdefs__.values():
            if isinstance(attrdef, LocalGidAttr):
                assert localattr is None, "redefined local gid attr"
                localattr = attrdef
                self.__attrdefs__.pop(attrdef.name, None)

            elif isinstance(attrdef, RemoteGidAttr):
                assert remoteattr is None, "redefined remote gid attr"
                remoteattr = attrdef
                self.__attrdefs__.pop(attrdef.name, None)

        assert (localattr and remoteattr) or name is 'EdgeData', "local or remote attrs missing"
        self.__localattr__ = localattr
        self.__remoteattr__ = remoteattr
        self.__edgetype__ = edgetype

        indexdefs = self.__dict__.get('__indexdefs__', [])
        self.__indexdefs__ = []

        for parent in cls_parents:
            for indexdef in copy.deepcopy(parent.__indexdefs__):
                self.addIndex(indexdef)

        for indexdef in indexdefs:
            self.addIndex(indexdef)

    def addIndex(self, indexdef):
            attrdefs = indexdef.attrdefs
            assert isinstance(indexdef, Index), "non Index type in index"
            indexname = '{}:{}'.format(self.__name__, ':'.join(attrdef.name for attrdef in attrdefs))
            indexdef.indextype = DATASTORE.addOrGetDefinitionType(indexname)
            self.__indexdefs__.append(indexdef)

    def __call__(self, localgid, remotegid, **attrs):
        assert self is not EdgeData, "cannot instantiate EdgeData directly, must inherit"

        # check if the instance is cached
        instance_key = (self, localgid, remotegid)
        instance = EdgeDataType._instanceCache.get(instance_key)

        if not instance:
            # create a new instance if one isn't in the instance cache
            instance = super(DataType, self).__call__(localgid, remotegid)
            EdgeDataType._instanceCache[instance_key] = instance

        # set the instance as locked if accessed under a lock
        if self.isLocked(self.colo(localgid)):
            instance.__locked__ = True
            self._lockedInstances.add(instance)

        if attrs:
            # clear and populate attributes for instance
            # can only do this once it is locked
            instance.initialize()
            instance.populate(attrs)
        else:
            # do not reset attributes if new ones are not provided
            pass

        return instance

    @classmethod
    def getEdgeDataClass(cls, edgetype):
        return cls._edgedataClasses.get(edgetype)

    def __getattr__(self, attrname):
        if self.__localattr__ and attrname == self.__localattr__.name:
            return self.__localattr__
        elif self.__remoteattr__ and attrname == self.__remoteattr__.name:
            return self.__remoteattr__

        return super(EdgeDataType, self).__getattr__(attrname)

class EdgeData(Data):

    lastAddWasOverwrite = False

    __metaclass__ = EdgeDataType

    #
    # caches queries made so far so we can reuse them
    # colo -> (edgetype, localgid) -> (None|index|remotegid) -> result
    #
    _queryCache = defaultdict(lambda: defaultdict(dict))
    _queryCacheDisabled = False

    # instances fetched under lock
    _lockedInstances = set()

    # instances that are dirtied or added under a lock
    _saveInstances = set()

    # instances that are deleted under a lock
    _deleteInstances = set()

    # colos that are currently locked
    _lockedColos = set()

    # list of encoders
    _encoders = [escode]
    _currentEncodingIndex = 0

    def __init__(self, localgid, remotegid, **attrs):

        # setattr forbids setting these, so set them directly in __dict__
        self.__dict__[self.__localattr__.name] = localgid
        self.__dict__[self.__remoteattr__.name] = remotegid

        # datastore sync variables
        self.__committeddatadict__ = None
        self.__committedrevision__ = 0
        self.__revision__ = 0

        # instance status variables
        self.__locked__ = False
        self.__save__ = False
        self.__delete__ = False

        # initialize data attributes
        super(EdgeData, self).__init__(**attrs)

    @property
    def __localgid__(self):
        return getattr(self, self.__localattr__.name)

    @property
    def __remotegid__(self):
        return getattr(self, self.__remoteattr__.name)

    @classmethod
    def generateGid(cls, colo_gid=None, colo=None):
        return DATASTORE.generateGid(colo_gid, colo)

    @classmethod
    def add(cls, **attrs):
        # extract `get` keyword argument
        get = attrs.pop('get', False)

        # extract local and remote gids
        localgid = attrs.pop(cls.__localattr__.name, None)
        remotegid = attrs.pop(cls.__remoteattr__.name, None)
        assert localgid and remotegid, "local(%d) or remote(%d) missing" % (localgid, remotegid)

        # make sure we are under the correct lock
        cls.checkLock(localgid, required=True)

        # check if data is in the datastore
        instance = cls.get(localgid, remotegid)
        assert not instance or get, "duplicate data (%s,%s,%s)" % (cls, localgid, remotegid)

        if not instance:
            instance = cls(localgid, remotegid, **attrs)
            instance._markSave()

        return instance

    @classmethod
    def delete(cls, localgid, remotegid):
        assert localgid and remotegid, "local(%d) or remote(%d) gid missing" % (localgid, remotegid)
        colo = cls.checkLock(localgid, required=True)
        instance = cls.get(localgid, remotegid)
        instance and instance._markDelete()

    @classmethod
    def get(cls, localgid, remotegid):
        assert localgid and remotegid, "local(%d) or remote(%d) gid missing" % (localgid, remotegid)
        colo = cls.checkLock(localgid)

        # check cache
        cached = cls._getQueryCache(localgid, remotegid)
        if cached: return cached

        # get instance
        edgedata = DATASTORE.get(cls.__edgetype__, localgid, remotegid)
        instance = cls._getInstanceFromEdge(edgedata) if edgedata else None

        # update cache
        cls._setQueryCache(localgid, remotegid, instance)

        return instance

    @classmethod
    def count(cls, localgid):
        assert localgid, "local(%d) gid missing" % localgid
        colo = cls.checkLock(localgid)

        # check cache
        cached = cls._getQueryCache(localgid, '__count__')
        if cached: return cached

        # get count
        count = DATASTORE.count(cls.__edgetype__, localgid)

        # update cache
        cls._setQueryCache(localgid, '__count__', count)

        return count

    def refresh(self):
        if not self.__locked__:
            self.get(self.__localgid__, self.__remotegid__)
        return self

    def remove(self):
        return self.delete(self.__localgid__, self.__remotegid__)

    @classmethod
    def query(cls, *args, **kwargs):
        return Query(cls, *args, **kwargs)

    @classmethod
    def queryfetch(cls, query):
        # check locks
        assert query.colo or not cls.insideLock(), "global query inside lock forbidden"
        query.colo and cls.checkLock(colo=query.colo)

        # index range
        indexrange = None
        if query.isindexquery:
            indexdef = first(indexdef for indexdef in cls.__indexdefs__ if indexdef.match(query))
            indexrange = query.range(indexdef)

        # check cache
        cached = cls._getQueryCache(query.localgid, indexrange, colo=query.colo)
        if cached: return list(cached)

        # fetch list
        edgedatas = DATASTORE.query(
            cls.__edgetype__, indexrange, gid1=query.localgid, colo=query.colo)
        instances = [cls._getInstanceFromEdge(edgedata) for edgedata in edgedatas]

        # update cache
        cls._setQueryCache(query.localgid, indexrange, instances, colo=query.colo)
        for instance in instances:
            cls._setQueryCache(instance.__localgid__, instance.__remotegid__, instance)

        return instances

    def __setattr__(self, attrname, attrvalue):
        # cannot assign localgid or remotegid
        assert attrname not in (self.__localattr__.name, self.__remoteattr__.name)

        if attrname in self.__attrdefs__:
            assert self.__locked__, "cannot make changes without a lock "
            self._markSave()

        return super(EdgeData, self).__setattr__(attrname, attrvalue)

    def __getattr__(self, attrname):
        if attrname in self.__attrdefs__ and self.insideLock():
            assert self.__locked__, "using unlocked data inside lock"

        return super(EdgeData, self).__getattr__(attrname)

    def _save(self):
        assert self.__locked__, "lock data before changes"
        assert self.__save__, "unexpected: unchanged instance being saved"

        # validate and encode the data
        encoding = EdgeData._currentEncodingIndex
        data = EdgeData._encoders[encoding].encode(self.dict(validate=True))
        indices = [indextuple for index in self.__indexdefs__ for indextuple in index.tuples(self)]

        # only overwrite data if we already have a previous revision
        overwrite = bool(self.__revision__)

        # add the edge and save whether it was an overwrite
        edgedata = DATASTORE.add(
            self.__edgetype__, self.__localgid__, self.__remotegid__,
            encoding, data, indices, overwrite)

        self.__class__.lastAddWasOverwrite = DATASTORE.lastAddWasOverwrite

        # set the updated revision
        edgetype, order, revision, localgid, remotegid, encoding, data = edgedata
        self.__revision__ = revision

        # clear all cached queries that might include this instance
        # and add it as the result to the get query cache
        self._clearQueryCache(localgid=localgid)
        self._setQueryCache(localgid, remotegid, self)

    def _delete(self):
        indextypes = [indexdef.indextype for indexdef in self.__indexdefs__]
        return DATASTORE.delete(
            self.__edgetype__, self.__localgid__, self.__remotegid__, indextypes)

    @classmethod
    def _getInstanceFromEdge(cls, edgedata):
        edgetype, order, revision, localgid, remotegid, encoding, data = edgedata
        instance = cls(localgid, remotegid)

        # update the instance if we don't have the most recent revision
        if instance.__revision__ < revision:
            encoder = EdgeData._encoders[encoding]
            datadict = encoder.decode(data)

            # convert from base type for data dict and skip unknown attributes
            for attrname in datadict:
                attrdef = cls.__attrdefs__.get(attrname)
                if attrdef:
                    datadict[attrname] = attrdef._from_base_type(datadict[attrname])

            instance.__datadict__ = instance.__committeddatadict__ = datadict
            instance.__revision__ = instance.__committedrevision__ = revision

        return instance

    @staticmethod
    @contextlib.contextmanager
    def disabledQueryCache():
        # disable query cache
        queryCacheDisabledOld = EdgeData._queryCacheDisabled
        EdgeData._queryCacheDisabled = True

        try:
            yield
        finally:
            # only the outermost with statemnt will set this back to its original
            # value since all others will see a disabled=True as the old value
            EdgeData._queryCacheDisabled = queryCacheDisabledOld

    @classmethod
    def colo(cls, gid):
        return DATASTORE.colo(gid)

    @staticmethod
    def isLocked(colo):
        return colo in EdgeData._lockedColos

    @staticmethod
    def insideLock():
        return EdgeData._lockedColos

    @classmethod
    def checkLock(cls, localgid=None, required=False, colo=None):
        # ensure we're under a correct lock before accessing instances
        assert not (localgid and colo), "unexpected: cannot check both localgid and colo"
        colo = colo or cls.colo(localgid)
        inside_lock = cls.insideLock()
        assert (not required and not inside_lock) or cls.isLocked(colo), "lock required"
        return colo

    @staticmethod
    @contextlib.contextmanager
    def lock(gids=[], colos=[]):

        gids = gids if isinstance(gids, list) else [gids]
        colos = colos and set(colos) or set(map(EdgeData.colo, gids))

        # empty lock waits for the first non empty lock
        if not colos:
            yield
            return

        # nested locks are noops
        if EdgeData.insideLock():
            assert colos.issubset(EdgeData._lockedColos), "cannot acquire new locks inside a lock"
            yield
            return

        EdgeData._lockedColos = colos
        save_instances = EdgeData._saveInstances
        delete_instances = EdgeData._deleteInstances
        locked_instances = EdgeData._lockedInstances

        assert not (save_instances or delete_instances or locked_instances)

        # clear the query cache
        for colo in colos:
            EdgeData._clearQueryCache(colo)

        try:

            with contextlib.nested(*map(DATASTORE.lock, sorted(EdgeData._lockedColos))):
                # all updates, adds and deletes will be stored in
                # save_instances and delete_instances
                yield

                # save any save instances
                for instance in save_instances:
                    instance._save()

                # save any save instances
                for instance in delete_instances:
                    instance._delete()

        except:

            # revert all changes, clear the cache again because add/get/list might
            # have populated it using new and changed data. also re-raise the exception

            for colo in colos:
                EdgeData._clearQueryCache(colo)

            for instance in save_instances:
                instance.__datadict__ = instance.__committeddatadict__
                instance.__revision__ = instance.__committedrevision__

            raise

        else:

            # store a copy of the freshly committed data dict and revision
            # this will be used to revert back to a committed state on future
            # changes

            for instance in save_instances:
                instance.__committeddatadict__ = dict(instance.__datadict__)
                instance.__committedrevision__ = instance.__revision__

        finally:

            # none of the instances are any longer locked
            for instance in locked_instances:
                instance.__locked__ = False
            locked_instances.clear()

            # none of the instances are any longer save
            for instance in save_instances:
                instance.__save__ = False
            save_instances.clear()

            # none of the instances are any longer deleted
            for instance in delete_instances:
                instance.__delete__ = False
            delete_instances.clear()

            EdgeData._lockedColos.clear()

    @contextlib.contextmanager
    def locknload(self):
        with self.lock(self.__localgid__):
            self.refresh()
            yield

    def _markDelete(self):
        if not self.__delete__:
            self.__delete__ = True
            EdgeData._deleteInstances.add(self)

    def _markSave(self):
        if not self.__save__:
            self.__save__ = True
            EdgeData._saveInstances.add(self)

    @staticmethod
    def clearInstanceCache():
        EdgeData._instanceCache.clear()

    @classmethod
    def _getQueryCache(cls, localgid, query, colo=None):
        colo = colo or cls.colo(localgid)
        cache = EdgeData._queryCache[colo][(cls.__edgetype__, localgid)]
        if not EdgeData._queryCacheDisabled and query in cache:
            return cache[query]

    @classmethod
    def _setQueryCache(cls, localgid, query, value, colo=None):
        colo = colo or cls.colo(localgid)
        EdgeData._queryCache[colo][(cls.__edgetype__, localgid)][query] = value

    @classmethod
    def _clearQueryCache(cls, colo=None, localgid=None):
        colo = colo or cls.colo(localgid)
        cache = EdgeData._queryCache[colo]
        if localgid:
            cache = cache[(cls.__edgetype__, localgid)]
        cache.clear()

    @staticmethod
    def clearQueryCache():
        EdgeData._queryCache.clear()

    def debug_print(self, prefix=''):
        super(EdgeData, self).debug_print()
        localgidname = self.__localattr__.name
        remotegidname = self.__remoteattr__.name
        padding = max(len(localgidname), len(remotegidname), 10)
        format = "%s    %%%ds: %%s" % (prefix, padding)
        print format % (localgidname, getattr(self, localgidname))
        print format % (remotegidname, getattr(self, remotegidname))
        print format % ('_revision', self.__revision__)
