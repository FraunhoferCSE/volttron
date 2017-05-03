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
        
        if table_names:
            self.data_table = table_names['data_table']
            self.topics_table = table_names['topics_table']
            self.meta_table = table_names['meta_table']
            self.agg_topics_table = table_names.get('agg_topics_table', None)
            self.agg_meta_table = table_names.get('agg_meta_table', None)
        # milliseconds work, though. 
        self.MICROSECOND_SUPPORT = True
        # .connect() method is the only thing used here. 
        super(MonetSqlFuncts, self).__init__(
            'monetdb.sql',
            **connect_params)

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
                topic_id INTEGER NOT NULL, \
                value_string TEXT NOT NULL, \
                     UNIQUE(topic_id, ts))')
            
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
                      "statements"

            raise RuntimeError(err_msg + ',' + repr(err))
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

    # thus far. 
        
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

        query = '''SELECT topic_id, ts, value_string
                FROM ''' + table_name + '''
                {where}
                {order_by}
                {limit}
                {offset}'''
        # this far:
        # just have to harmonize with how MonetDB does finer timestamps.
        
        if self.MICROSECOND_SUPPORT is None:
            self.init_microsecond_support()

        where_clauses = ["WHERE topic_id = %s"]
        args = [topic_ids[0]]

        if start is not None:
            if not self.MICROSECOND_SUPPORT:
                start_str = start.isoformat()
                start = start_str[:start_str.rfind('.')]

        if end is not None:
            if not self.MICROSECOND_SUPPORT:
                end_str = end.isoformat()
                end = end_str[:end_str.rfind('.')]

        if start and end and start == end:
            where_clauses.append("ts = %s")
            args.append(start)
        else:
            if start:
                where_clauses.append("ts >= %s")
                args.append(start)
            if end:
                where_clauses.append("ts < %s")
                args.append(end)
            
        where_statement = ' AND '.join(where_clauses)
        print(where_statement)
        order_by = 'ORDER BY ts ASC'
        if order == 'LAST_TO_FIRST':
            order_by = ' ORDER BY topic_id DESC, ts DESC'

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
            args[0] = topic_id
            real_query = query.format(where=where_statement,
                                      limit=limit_statement,
                                      offset=offset_statement,
                                      order_by=order_by)
            _log.debug("Real Query: " + real_query)
            _log.debug("args: " + str(args))

            rows = self.select(real_query, args)
            if rows:
                for _id, ts, value in rows:
                    values[id_name_map[topic_id]].append(
                        (utils.format_timestamp(ts.replace(tzinfo=pytz.UTC)),
                         jsonapi.loads(value)))
            _log.debug("query result values {}".format(values))
        return values
        
        

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
        #monet.setup_historian_tables()
        #monet.record_table_definitions( defs, "volttron_table_definitions")
        #monet.setup_aggregate_historian_tables("volttron_table_definitions")
        monet.query([1], {1:'foo'},
                    start = isodate.parse_datetime('2017-04-22T17:55:00'),
                    end = isodate.parse_datetime('2017-04-22T18:55:00'),

        )
    except Exception as e:
        print e
        #monet.execute_stmt("drop table " + defs['data_table']+' ;')
        #monet.execute_stmt("drop table " + defs['meta_table']+' ;')
        #monet.execute_stmt("drop table " + defs['topics_table']+' ;')
        #monet.execute_stmt("drop table volttron_table_definitions;")
        raise e
if __name__ == '__main__':
    # Entry point for script
    main(sys.argv)
