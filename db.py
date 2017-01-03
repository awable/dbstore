import copy
import time
import MySQLdb
import MySQLdb.converters
import MySQLdb.cursors
import MySQLdb.constants

from contextlib import contextmanager, closing

class DB:


    # MySQL closes idle connections, so we'll refresh the
    # connection every 1 hour
    _recycleInterval = 60*60*1

    # Cache of DB instances
    _instances = {}

    @staticmethod
    def getInstance(host, dbname):
        instance = DB._instances.get((host, dbname))
        if not instance:
            instance = DB._instances[(host, dbname)] = DB(host, dbname)
        return instance

    def __init__(self, host, dbname):
        self._dbhost = host
        self._dbname = dbname
        self._dbconn = None
        self._connectTime = None
        self._dbuser = 'root'
        self._dbpasswd = ''
        self._affectedRows = 0
        self._lastInsertID = 0
        self._transactionDepth = 0
        self._transactionCursor = None

    def getConnection(self):
        if (not self._dbconn or
            not self._connectTime or
            long(time.time()) - self._connectTime > DB._recycleInterval):
            self.closeConnection()
            self._connectTime = long(time.time())
            self._dbconn = MySQLdb.connect(
                host=self._dbhost,
                db=self._dbname,
                user=self._dbuser,
                passwd=self._dbpasswd,
                conv=DB._getConversions(),
                use_unicode=True,
                charset='utf8',
                init_command='SET time_zone = "+0:00"',
                sql_mode='TRADITIONAL')
            self._dbconn.autocommit(True)
        return self._dbconn

    def closeConnection(self):
        if self._dbconn:
            self._dbconn.close()
            self._dbconn = None

    @contextmanager
    def transaction(self):
        try:
            self._startTransaction()
            yield
            self._commitTransaction()
        except:
            self._rollbackTransaction()
            raise

    def _startTransaction(self):
        if self._transactionDepth == 0:
            self.run('BEGIN')
        self._transactionDepth += 1

    def _commitTransaction(self):
        assert self._transactionDepth, "Commit called on non-existant transaction"
        if self._transactionDepth == 1:
            self.getConnection().commit()
        self._transactionDepth -= 1

    def _rollbackTransaction(self):
        if self._transactionDepth > 0:
            self.getConnection().rollback()
        self._transactionDepth = 0

    def run(self, sql, args=None):
        with closing(self.getConnection().cursor()) as cursor:
            return self._execute(cursor, sql, args)

    def get(self, sql, args=None):
        with closing(self.getConnection().cursor()) as cursor:
            self._execute(cursor, sql, args)
            return cursor.fetchall()

    def getOne(self, sql, args=None):
        with closing(self.getConnection().cursor()) as cursor:
            self._execute(cursor, sql, args)
            for row in cursor: return row

    def _execute(self, cursor, sql, args):
        try:
            return cursor.execute(sql, args)
        except:
            self.closeConnection()
            raise
        finally:
            self._affectedRows = cursor.rowcount
            self._lastInsertID = cursor.lastrowid

    def getAffectedRows(self):
        return self._affectedRows

    def getLastInsertID(self):
        return self._lastInsertID

    def hasOngoingTransaction(self):
        return bool(self._transactionDepth)

    @staticmethod
    def _getConversions():
        # Fix the access conversions to properly recognize unicode/binary
        MYSQL_FIELD_TYPE = MySQLdb.constants.FIELD_TYPE
        MYSQL_FLAG = MySQLdb.constants.FLAG

        field_types = [
            MYSQL_FIELD_TYPE.BLOB,
            MYSQL_FIELD_TYPE.STRING,
            MYSQL_FIELD_TYPE.VAR_STRING,
            MYSQL_FIELD_TYPE.VARCHAR]

        conversions = copy.deepcopy(MySQLdb.converters.conversions)
        for field_type in field_types:
            conversions[field_type] = [(MYSQL_FLAG.BINARY, str)] +  conversions[field_type]

        return conversions
