import ast
import logging
from collections import defaultdict
import monetdb.sql
import sys
import pytz,isodate
import re
from basedb import DbDriver
from volttron.platform.agent import utils
from zmq.utils import jsonapi
import traceback
import threading
utils.setup_logging()
_log = logging.getLogger(__name__)

"""
Implementation of Mysql database operation for
:py:class:`sqlhistorian.historian.SQLHistorian` and
:py:class:`sqlaggregator.aggregator.SQLAggregateHistorian`
For method details please refer to base class
:py:class:`volttron.platform.dbutils.basedb.DbDriver`
"""
class MonetSqlFuncts(DbDriver):
    def __init__(self, connect_params, table_names):
        self.data_table = None
        self.topics_table = None
        self.meta_table = None
        self.agg_topics_table = None
        self.agg_meta_table = None
        self.tses = []
        if table_names:
            self.data_table = table_names['data_table']
            self.topics_table = table_names['topics_table']
            self.meta_table = table_names['meta_table']
            self.agg_topics_table = table_names.get('agg_topics_table', None)
            self.agg_meta_table = table_names.get('agg_meta_table', None)
        # milliseconds work, though. 
        self.MICROSECOND_SUPPORT = True
        self.insert_lock = threading.Lock()
        # .connect() method is the only thing used here. 
        super(MonetSqlFuncts, self).__init__(
            'monetdb.sql',
            **connect_params)
        self.load_coltypes()
        
    def setup_historian_tables(self):
        """
        TODO: add tests for existence. 

        """
        rows = map(
            lambda x:x[0],
            self.select("select name from sys.tables where system=0;", []))
        if self.data_table in rows:
            _log.debug("Found table {}. Historian table exists".format(
                self.data_table))
            return
        try:
            # posint of 6 ?  
            self.execute_stmt(
                'CREATE TABLE ' + self.data_table +
                ' (ts timestamp(3) NOT NULL,\
                     PRIMARY KEY ( ts))')
            
            #self.execute_stmt('''CREATE INDEX data_idx
            #                        ON ''' + self.data_table + ''' (ts)''')
            self.execute_stmt('''CREATE TABLE ''' +
                              self.topics_table +
                              ''' (topic_id INTEGER NOT NULL AUTO_INCREMENT,
                                   topic_name varchar(512) NOT NULL,
                                   PRIMARY KEY (topic_id),
                                   UNIQUE(topic_name))''')
            self.execute_stmt('''CREATE TABLE '''
                              + self.meta_table +
                              '''(topic_id INTEGER NOT NULL,
                               metadata TEXT NOT NULL,
                               PRIMARY KEY(topic_id))''')
            _log.debug("Created data topics and meta tables")

            self.commit()
        except Exception as err:
            err_msg = "Error creating " \
                      "historian tables as the configured user. " \
                      "Please create the tables manually before " \
                      "restarting historian. Please refer to " \
                      "monet-create*.sql files for create " \
                      "statements. "
            _log.error(err_msg + repr(err))
            #raise RuntimeError(err_msg + ',' + repr(err))
    def record_table_definitions(self, tables_def, meta_table_name):
        _log.debug(
            "In record_table_def {} {}".format(tables_def, meta_table_name))
        rows = map(
            lambda x:x[0],
            self.select("select name from sys.tables where system=0;", []))
        table_prefix = tables_def.get('table_prefix', "")
        if meta_table_name not in rows:
            _log.debug("Found table {}. Historian table exists".format(
                self.data_table))
            self.execute_stmt(
                'CREATE TABLE ' + meta_table_name +
                ' (table_id varchar(512) PRIMARY KEY, \
                table_name varchar(512) NOT NULL, \
                table_prefix varchar(512));')
            insert_stmt = 'INSERT INTO ' + meta_table_name + \
                          " VALUES (%s, %s, %s );"
            for k in ['data_table','topics_table','meta_table']:
                self.insert_stmt(insert_stmt,
                             (k,
                              tables_def[k],
                              table_prefix))
        else:
            insert_stmt = 'UPDATE ' + meta_table_name + \
                          " SET table_name=%s, table_prefix=%s where table_id=%s ;"
            for k in ['data_table','topics_table','meta_table']:
                self.insert_stmt(insert_stmt,
                             (tables_def[k],
                              table_prefix, k))
        self.commit()

        
    def setup_aggregate_historian_tables(self, meta_table_name):
        table_names = self.read_tablenames_from_db(meta_table_name)
        print(table_names)
        self.data_table = table_names['data_table']
        self.topics_table = table_names['topics_table']
        _log.debug("In setup_aggregate_historian self.topics_table"
                   " {}".format(self.topics_table))
        self.meta_table = table_names['meta_table']
        self.agg_topics_table = table_names.get('agg_topics_table', None)
        self.agg_meta_table = table_names.get('agg_meta_table', None)
        rows = map(
            lambda x:x[0],
            self.select("select name from sys.tables where system=0;", []))
        if self.agg_topics_table not in rows:

            self.execute_stmt(
                'CREATE TABLE ' + self.agg_topics_table +
                ' (agg_topic_id INTEGER NOT NULL AUTO_INCREMENT, \
                agg_topic_name varchar(512) NOT NULL, \
                agg_type varchar(512) NOT NULL, \
                agg_time_period varchar(512) NOT NULL, \
                PRIMARY KEY (agg_topic_id), \
                UNIQUE(agg_topic_name, agg_type, agg_time_period));')
        if self.agg_meta_table not in rows:
            self.execute_stmt(
                'CREATE TABLE ' + self.agg_meta_table +
                '(agg_topic_id INTEGER NOT NULL, \
                metadata TEXT NOT NULL, \
                PRIMARY KEY(agg_topic_id));')
            
        _log.debug("Created aggregate topics and meta tables")

    def query(self, topic_ids, id_name_map, start=None, end=None, skip=0,
              agg_type=None,
              agg_period=None, count=None, order="FIRST_TO_LAST"):

        table_name = self.data_table
        if agg_type and agg_period:
            table_name = agg_type + "_" + agg_period

        query = '''SELECT ts, topic_{topic_id}_  
                FROM ''' + table_name + '''
                {where}
                {order_by}
                {limit}
                {offset}'''
        # this far:
        # just have to harmonize with how MonetDB does finer timestamps.
        
        if self.MICROSECOND_SUPPORT is None:
            self.init_microsecond_support()

        where_clauses = [" where topic_{topic_id}_ is not null"]

        args = []

        if start is not None:
            if not self.MICROSECOND_SUPPORT:
                start_str = start.isoformat()
                start = start_str[:start_str.rfind('.')]

        if end is not None:
            if not self.MICROSECOND_SUPPORT:
                end_str = end.isoformat()
                end = end_str[:end_str.rfind('.')]

        if start and end and start == end:
            where_clauses.append("ts = '{start}'")
            #args.append(start)
        else:
            if start:
                where_clauses.append("ts >= '{start}'")
                #args.append(start)
            if end:
                where_clauses.append("ts < '{end}'")
                #args.append(end)
            
        order_by = 'ORDER BY ts ASC'
        if order == 'LAST_TO_FIRST':
            order_by = ' ORDER BY ts DESC'

        # can't have an offset without a limit
        # -1 = no limit and allows the user to
        # provide just an offset
        if count is None:
            count = 100

        limit_statement = 'LIMIT %s'
        args.append(int(count))

        offset_statement = ''
        if skip > 0:
            offset_statement = 'OFFSET %s'
            args.append(skip)

        _log.debug("About to do real_query")
        values = defaultdict(list)
        for topic_id in topic_ids:
            #args[0] = topic_id
            where_statement = (' AND '.join(where_clauses)).format(
                topic_id=topic_id,start=start,end=end
            )
            _log.error("Query: " + query)
            real_query = query.format(where=where_statement,                                      
                                      limit=limit_statement,
                                      offset=offset_statement,
                                      topic_id=topic_id,
                                      order_by=order_by)
            _log.error("Real Query: " + real_query)
            _log.error("args: " + str(args))

            rows = self.select(real_query, args)
            if rows:
                if self.coltypes.get("topic_%s_"%topic_id) == 'clob':
                    for ts, value in rows:
                        values[id_name_map[topic_id]].append(
                            (utils.format_timestamp(
                                ts.replace(tzinfo=pytz.UTC)),
                             jsonapi.loads(value)
                            ))
                else:
                    for ts, value in rows:
                        values[id_name_map[topic_id]].append(
                            (utils.format_timestamp(
                                ts.replace(tzinfo=pytz.UTC)),
                             value
                            ))                    
            _log.debug("query result values {}".format(values))
        return values
        
    def insert_meta(self, topic_id, metadata):
        """
        Inserts metadata for topic

        :param topic_id: topic id for which metadata is inserted
        :param metadata: metadata
        :return: True if execution completes. False if unable to connect to
                 database
        """
        #if not self.__connect():
        #    return False
        try: 
            self.insert_stmt(
                 '''INSERT INTO ''' + self.meta_table + ''' values(%s, %s)''',
                (topic_id, jsonapi.dumps(metadata)))
        except monetdb.sql.OperationalError as e:
            self.rollback()
            self.insert_stmt(
                "update "+ self.meta_table + " set metadata=%s where topic_id=%s ",
                (jsonapi.dumps(metadata), topic_id))
        self.commit()
        return True
        
    def insert_topic(self, topic_name, **kwargs):
        """
        Insert a new topic

        :param topic: topic to insert
        :return: id of the topic inserted if insert was successful.
                 False if unable to connect to database
        """
        try:
            ret = self.insert_stmt(
                '''INSERT INTO ''' +
                self.topics_table +
                ''' ( topic_name) values (%s)''',
                ( topic_name,))
            _log.debug("In insert_topic - self.topic_table "
                       "{}".format(self.topics_table))
            topic_id = (ret  if ret is not False else False)
            _log.debug("Topic_id {} {}".format(topic_id, kwargs))
            self.commit()
            coltype = kwargs.get("meta",{}).get("type")
            coltype = {
                "float":"DOUBLE",
                "integer":"INTEGER",
            }.get(coltype,"TEXT")
            self.execute_stmt(
                '''ALTER TABLE '''+ self.data_table +
                ''' ADD COLUMN topic_%s_ %s ; '''%(topic_id,coltype))
            self.commit()
        except monetdb.sql.OperationalError as e:
            self.rollback()
            _log.error("ROLLBACK {}".format(e))
            topic_id = self.select("SELECT topic_id from topics where topic_name='%s';"%topic_name,[])[0][0]
        self.load_coltypes()    
        return [topic_id]
        
    def insert_topic_query(self):
        # XXX: alter table data add column
        _log.debug("In insert_topic_query - self.topic_table "
                   "{}".format(self.topics_table))
        return '''INSERT INTO ''' + self.topics_table + ''' (topic_name)
            VALUES (%s)'''
    
    def insert_data(self, ts, topic_id, data):
        """
        Inserts data for topic

        :param ts: timestamp
        :param topic_id: topic id for which data is inserted
        :param metadata: data values
        :return: True if execution completes. False if unable to connect to
                 database
        """
        datum = (jsonapi.dumps(data) if
                 self.coltypes.get('topic_%s_'%topic_id) == 'clob'
                 else data)
        if datum in ('nan',):
            return
        if ts not in self.tses:
            self.insert_lock.acquire()            
            try:
                ret = self.insert_stmt(
                    '''INSERT INTO ''' +
                    self.data_table +
                    ''' (ts, topic_%s_) values (%s, %s)''',
                    ( topic_id, ts, datum))
                _log.debug("INSERT DATA {} {} {} {}".format(ts, topic_id,data,ret))
                self.commit()
            except monetdb.sql.OperationalError as e:
                self.rollback()
                ret = self.insert_stmt(
                    "update "+ self.data_table +
                    " set topic_{}_=%s where ts=%s".format(topic_id),
                    (datum, ts ))
                self.commit()
            self.tses.append(ts)
            if len(self.tses)> 512:
                self.tses.pop(0)
            self.insert_lock.release()
        else:
            self.insert_lock.acquire()            
            ret = self.insert_stmt(
                "update "+ self.data_table +
                " set topic_{}_=%s where ts=%s".format(topic_id),
                (datum, ts ))                    
            self.insert_lock.release()
        return True
            
    def update_topic_query(self):
        return '''UPDATE ''' + self.topics_table + ''' SET topic_name = %s
            WHERE topic_id = %s'''
    
    def insert_agg_topic_stmt(self):
        _log.debug("Insert aggregate topics stmt inserts "
                   "into {}".format(self.agg_topics_table))
        return '''INSERT INTO ''' + self.agg_topics_table + '''
            (agg_topic_name, agg_type, agg_time_period )
            values (%s, %s, %s)'''

    def update_agg_topic_stmt(self):
        return '''UPDATE ''' + self.agg_topics_table + ''' SET
        agg_topic_name = %s WHERE agg_topic_id = %s '''

    def load_coltypes(self):
        """
        Use the sys.tables and sys.columns 
        information to 
        """
        meta = self.select("select id from sys.tables where name='data';",[])
        if not meta:
            return
        self.coltypes = dict(
            self.select(
                "select name,type from sys.columns where table_id=%s;"%meta[0][0],[])
            )
        _log.debug("COLTYPES: {}".format(self.coltypes))
        
    def get_topic_map(self):
        q = "SELECT topic_id, topic_name FROM " + self.topics_table + ";"
        rows = self.select(q, None)
        _log.debug("loading topic map from db")
        id_map = dict()
        name_map = dict()
        for t, n in rows:
            id_map[n.lower()] = t
            name_map[n.lower()] = n
        _log.debug(id_map)
        _log.debug(name_map)
        self.load_coltypes()
        return id_map, name_map
            
def main(args):
    try:
        defs =         {
            'data_table':'data',
            'topics_table':'topics',
            'meta_table':'meta',
            "table_prefix":"meta_"
        }
        monet = MonetSqlFuncts(
            { "username":"volttron","password":"shines","database":"volttron","hostname":"localhost"},
            defs)
        _log.setLevel(logging.DEBUG)
        monet.setup_historian_tables()
        monet.record_table_definitions( defs, "volttron_table_definitions")
        #monet.setup_aggregate_historian_tables("volttron_table_definitions")
        monet.insert_topic("foo")
        return
        monet.query([1], {1:'foo'},
                    start = isodate.parse_datetime('2017-04-22T17:55:00'),
                    end = isodate.parse_datetime('2017-04-22T18:55:00'),

        )
        #monet.__connect()
        monet.insert_meta(177,{'foo':'bar'})
    except Exception as e:
        foo = (sys.exc_info())
        _log.info(traceback.extract_tb(foo[-1]))
        print e
        
        #monet.execute_stmt("drop table " + defs['data_table']+' ;')
        #monet.execute_stmt("drop table " + defs['meta_table']+' ;')
        #monet.execute_stmt("drop table " + defs['topics_table']+' ;')
        #monet.execute_stmt("drop table volttron_table_definitions;")
        raise e
if __name__ == '__main__':
    # Entry point for script
    main(sys.argv)
