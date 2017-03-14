import random

from itertools import chain
from db import DB
from config import config
from contextlib import contextmanager

class DataStore(object):

    # keep ids in 32 bit range for the time being
    _MAX_COLO_ID = (1 << 32) - 1
    _NUM_HOSTS = len(config.DATABASE_HOSTS)

    _instances = {}

    @staticmethod
    def getInstance(dbname=config.DATABASE_NAME):
        instance =  DataStore._instances.get(dbname)
        if not instance:
            instance = DataStore(dbname)
        return instance

    def __init__(self, dbname):
        self._dbname = dbname
        self._shards = {}
        self._locked_colos = set()
        self.lastAddWasOverwrite = False
        self.definitionsDB = DB.getInstance(config.DEFINITIONS_HOST, self._dbname)

    def colo(self, gid):
        return gid >> 32

    def generateGid(self, colo_gid=None, colo=None):
        assert not (colo_gid and colo), "cannot specify both colo and colo_gid"
        colo = (
            colo
            or (colo_gid and self.colo(colo_gid))
            or random.randrange(1, self._MAX_COLO_ID + 1))
        return self._getColoShard(colo).generateGid(colo)

    def add(self, edgetype, gid1, gid2, encoding, data, indices=[], overwrite=False):
        shard = self._getShard(gid1)
        edge = shard.add(edgetype, gid1, gid2, encoding, data, indices, overwrite)
        self.lastAddWasOverwrite = shard.lastAddWasOverwrite
        return edge

    def delete(self, edgetype, gid1, gid2, indextypes=[]):
        return self._getShard(gid1).delete(edgetype, gid1, gid2, indextypes)

    def query(self, edgetype, index=None, gid1=None, colo=None):
        assert not (gid1 and colo), "cannot query with both parent gid and colo"

        if colo or gid1:
            colo = colo or self.colo(gid1)
            return self._getColoShard(colo).query(edgetype, index, gid1)

        results = [
            self._getHostShard(hostindex).query(edgetype, index, None)
            for hostindex in range(self._NUM_HOSTS)]

        return list(headpq.merge(*results))

    def get(self, edgetype, gid1, gid2, index=None):
        return self._getShard(gid1).get(edgetype, gid1, gid2, index)

    def count(self, edgetype, gid1):
        return self._getShard(gid1).count(edgetype, gid1)

    def insideLock(self):
        return len(self._locks)

    def isLocked(self, edgetype, gid1):
        return (edgetype, gid1) in self._locks

    @contextmanager
    def lock(self, colo):
        if colo in self._locked_colos:
            # nested locks are noops
            yield
            return

        try:
            self._locked_colos.add(colo)
            shard = self._getColoShard(colo)
            with shard.transaction():
                yield shard.lock(colo)
        finally:
            self._locked_colos.remove(colo)

    def _getShard(self, gid):
        return self._getColoShard(self.colo(gid))

    def _getColoShard(self, colo):
        return self._getHostShard(colo % self._NUM_HOSTS)

    def _getHostShard(self, hostindex):
        db = DB.getInstance(config.DATABASE_HOSTS[hostindex], self._dbname)
        shard = self._shards.get(db)
        if not shard:
            shard = self._shards[db] = DataStoreShard(db)
        return shard

    _addDefinitionSQL = """
        INSERT INTO definitions
        (`name`, `typeid`)
        VALUES (%s, NULL)
        ON DUPLICATE KEY
        UPDATE typeid = LAST_INSERT_ID(typeid)
    """

    _getDefinitionSQL = """
        SELECT typeid
        FROM definitions
        WHERE `name` = %s
    """

    def addOrGetDefinitionType(self, name):
        self.definitionsDB.run(self._addDefinitionSQL, (name,))
        return self.definitionsDB.getLastInsertID()

    def getDefinitionType(self, name):
        return self.definitionsDB.getOne(self.getDefinitionSQL, name)

class DataStoreShard(object):

    def __init__(self, db):
        self._db = db
        self.lastAddWasOverwrite = False

    _generateGidSQL = """
       INSERT INTO colo
       (`colo`, `counter`)
       VALUES (%s, LAST_INSERT_ID(%s))
       ON DUPLICATE KEY
       UPDATE counter = LAST_INSERT_ID(counter + 1)
    """

    def generateGid(self, colo, start=1):
        self._db.run(self._generateGidSQL, (colo, start))
        return (colo << 32) + self._db.getLastInsertID()

    _addSQL = """
      INSERT INTO edgedata
      (edgetype, revision, gid1, gid2, encoding, data)
      VALUES (%s, LAST_INSERT_ID(%s), %s, %s, %s, %s)
    """

    _addOverwriteSQL = """
      INSERT INTO edgedata
      (edgetype, revision, gid1, gid2, encoding, data)
      VALUES (%s, LAST_INSERT_ID(%s), %s, %s, %s, %s)
      ON DUPLICATE KEY
      UPDATE data = VALUES(data),
         revision = LAST_INSERT_ID(revision),
         revision = VALUES(revision),
         encoding = VALUES(encoding),
             data = VALUES(data)
    """

    _uniqueIndexSQL = """
      SELECT COUNT(1)
      FROM edgeindex
      WHERE indextype = %s and indexvalue = %s
    """

    _deleteIndexSQL = """
      DELETE FROM edgeindex
      WHERE indextype = %s
        AND gid1 = %s
        AND revision = %s
    """

    _addIndexSQL = """
      INSERT INTO edgeindex
      (indextype, indexvalue, gid1, revision)
      VALUES (%s, %s, %s, %s)
    """

    def add(self, edgetype, gid1, gid2, encoding, data, indices=[], overwrite=False):
        with self._db.transaction():

            # get new revision
            revision = self._incrementRevision(edgetype, gid1)
            edgedata = (edgetype, 0, revision, gid1, gid2, encoding, data)
            edgeargs = (edgetype, revision, gid1, gid2, encoding, data)

            add_sql = DataStoreShard._addOverwriteSQL if overwrite else DataStoreShard._addSQL
            self._db.run(add_sql, edgeargs)

            affected_rows = self._db.getAffectedRows()
            prev_revision = self._db.getLastInsertID()

            # if the edge was added, increment edge count
            if affected_rows == 1:
                self._incrementCount(edgetype, gid1)
                assert prev_revision == revision, "added edge should not have a previous revision"
            elif affected_rows == 2:
                self.lastAddWasOverwrite = True
                assert prev_revision == (revision - 1), "data changed during update"

            for indextype, indexvalue, unique in indices:

                # if the edge already existed, delete old indices
                if affected_rows == 2:
                    self._db.run(DataStoreShard._deleteIndexSQL, (indextype, gid1, prev_revision))

                if unique:
                    count = self._db.getOne(DataStoreShard._uniqueIndexSQL, (indextype, indexvalue))
                    assert not count[0], "edge violates index uniqueness"

                self._db.run(DataStoreShard._addIndexSQL, (indextype, indexvalue, gid1, revision))

            return edgedata

    _deleteSQL = """
      DELETE FROM edgedata
      WHERE edgetype = %s
        AND gid1 = %s
        AND gid2 = %s
        AND revision = LAST_INSERT_ID(revision)
    """

    _lastInsertIDSQL = "SELECT LAST_INSERT_ID()"

    def delete(self, edgetype, gid1, gid2, indextypes=[]):
        with self._db.transaction():

            # increment revision since we are making a change
            self._incrementRevision(edgetype, gid1)
            self._db.run(DataStoreShard._deleteSQL, (edgetype, gid1, gid2))
            affected_rows = self._db.getAffectedRows()

            # _mysql API doesn't update insertid on DELETE statements so we
            # explicitly fetch the LAST_INSERT_ID()
            del_revision = self._db.getOne(DataStoreShard._lastInsertIDSQL)[0]

            # decrement edge count and remove old indices if we actually deleted something
            if affected_rows:
                self._incrementCount(edgetype, gid1, -1)

                assert del_revision, "missing revision for deleted edge"
                for indextype in indextypes:
                    self._db.run(DataStoreShard._deleteIndexSQL, (indextype, gid1, del_revision))

            return (affected_rows == 1)


    _listSQL = """
      SELECT edgetype, 0, revision, gid1, gid2, encoding, data
      FROM edgedata
      WHERE edgetype = %s AND gid1 = %s
      ORDER BY revision DESC
    """

    _querySQL = """
      SELECT edgedata.edgetype,
             edgeindex.indexvalue,
             edgedata.revision,
             edgedata.gid1,
             edgedata.gid2,
             edgedata.encoding,
             edgedata.data
      FROM edgeindex STRAIGHT_JOIN edgedata
      ON (edgedata.edgetype = %s
        AND edgedata.gid1 = {}
        AND edgedata.revision = edgeindex.revision)
      WHERE edgeindex.indextype = %s
        AND edgeindex.indexvalue BETWEEN %s and %s
      ORDER BY edgeindex.indexvalue, edgeindex.revision DESC
    """

    def query(self, edge_type, index, gid1=None):
        if gid1 and not index:
            query = DataStoreShard._listSQL
            args = (edge_type, gid1)
        elif gid1:
            indextype, indexstart, indexend = index
            query = DataStoreShard._querySQL.format('%s')
            args = (edge_type, gid1, indextype, indexstart, indexend)
        else:
            indextype, indexstart, indexend = index
            query = DataStoreShard._querySQL.format('edgeindex.gid1')
            args = (edge_type, indextype, indexstart, indexend)

        return self._db.get(query, args)

    _getSQL = """
      SELECT edgetype, '', revision, gid1, gid2, encoding, data
      FROM edgedata
      WHERE edgetype = %s
        AND gid1 = %s
        AND gid2 = %s
    """

    _getIndexSQL = """
      SELECT edgedata.edgetype,
             edgeindex.indexvalue
             edgedata.revision,
             edgedata.gid1,
             edgedata.gid2,
             edgedata.encoding,
             edgedata.data,
      FROM edgeindex
      STRAIGHT_JOIN edgedata
      ON (edgedata.edgetype = %s
        AND edgedata.gid1 = %s
        AND edgedata.gid2 = %s
        AND edgedata.revision = edgeindex.revision)
      WHERE edgeindex.indextype = %s
        AND edgeindex.indexvalue BETWEEN %s AND %s
    """

    def get(self, edge_type, gid1, gid2, index=None):
        if index:
            indextype, indexstart, indexend = indexrange
            query = DataStoreShard._getIndexSQL
            args = (edge_type, gid1, gid2, indextype, indexstart, indexend)
        else:
            query = DataStoreShard._getSQL
            args = (edge_type, gid1, gid2)

        return self._db.getOne(query, args)


    _countSQL = """
      SELECT `count` from edgemeta
      WHERE edgetype = %s AND gid1 = %s
    """

    def count(self, edgetype, gid1):
        row = self._db.getOne(DataStoreShard._countSQL, (edgetype, gid1))
        return row[0] if row else 0

    def lock(self, colo):
        assert self._db.hasOngoingTransaction()
        return self.generateGid(colo, start=0)

    def transaction(self):
        return self._db.transaction()

    _incrementRevisionSQL = """
        INSERT INTO edgemeta
        (edgetype, gid1, revision, count)
        VALUES (%s, %s, LAST_INSERT_ID(1), 0)
        ON DUPLICATE KEY
        UPDATE revision = LAST_INSERT_ID(revision + 1)
    """

    def _incrementRevision(self, edgetype, gid1):
        self._db.run(DataStoreShard._incrementRevisionSQL, (edgetype, gid1))
        return self._db.getLastInsertID()

    _incrementCountSQL = """
        UPDATE edgemeta
        SET `count` = `count` + %s
        WHERE edgetype = %s AND gid1 = %s
    """

    def _incrementCount(self, edgetype, gid1, inc=1):
        self._db.run(DataStoreShard._incrementCountSQL, (inc, edgetype, gid1))
