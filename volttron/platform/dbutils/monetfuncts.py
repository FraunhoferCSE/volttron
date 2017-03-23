import ast
import logging
from collections import defaultdict
import monetdb.sql
import sys
import pytz
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
        self.MICROSECOND_SUPPORT = False
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
            
            self.execute_stmt('''CREATE INDEX data_idx
                                    ON ''' + self.data_table + ''' (ts)''')
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
    # thus far. 
    def record_table_definitions(self, tables_def, meta_table_name):
        _log.debug(
            "In record_table_def {} {}".format(tables_def, meta_table_name))
        rows = map(
            lambda x:x[0],
            self.select("select name from sys.tables where system=0;", []))
        if meta_table_name not in rows:
            _log.debug("Found table {}. Historian table exists".format(
                self.data_table))
            self.execute_stmt(
                'CREATE TABLE ' + meta_table_name +
                ' (table_id varchar(512) PRIMARY KEY, \
                table_name varchar(512) NOT NULL, \
                table_prefix varchar(512));')
            
        table_prefix = tables_def.get('table_prefix', "")
        # ???
        insert_stmt = 'UPDATE ' + meta_table_name + \
                      ' SET table_name="%s", table_prefix="%s" where table_id="%s";'
        self.insert_stmt(insert_stmt,
                         (tables_def['data_table'],
                          table_prefix, 'data_table'))
        self.insert_stmt(insert_stmt,
                         (tables_def['topics_table'],
                          table_prefix, 'topics_table'))
        self.insert_stmt(
            insert_stmt,
            (tables_def['meta_table'], table_prefix, 'meta_table'))
        self.commit()

def main(args):
    monet = MonetSqlFuncts(
        { "username":"volttron","password":"shines","database":"volttron","hostname":"localhost"},
        {
            'data_table':'data_test',
            'topics_table':'topics_test',
            'meta_table':'meta_test'
        })
    monet.setup_historian_tables()
    #monet.record_table_definitions()
    
if __name__ == '__main__':
    # Entry point for script
    main(sys.argv)