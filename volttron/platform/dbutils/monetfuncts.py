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
        1. Check for tables existing. 

        2. Create data_table if not.

        3. Create data_idx ? 

        4. Create topics_table

        5. Create meta table. 

        """
        try:
            # posint of 6 ?  
            self.execute_stmt(
                'CREATE TABLE IF NOT EXISTS ' + self.data_table +
                ' (ts timestamp(3) NOT NULL,\
                topic_id INTEGER NOT NULL, \
                value_string TEXT NOT NULL, \
                     UNIQUE(topic_id, ts))')
            
            self.execute_stmt('''CREATE INDEX data_idx
                                    ON ''' + self.data_table + ''' (ts ASC)''')
            self.execute_stmt('''CREATE TABLE IF NOT EXISTS ''' +
                              self.topics_table +
                              ''' (topic_id INTEGER NOT NULL AUTO_INCREMENT,
                                   topic_name varchar(512) NOT NULL,
                                   PRIMARY KEY (topic_id),
                                   UNIQUE(topic_name))''')
            self.execute_stmt('''CREATE TABLE IF NOT EXISTS '''
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
            raise RuntimeError(err_msg)

    
    return sched


def main(args):
    monet = MonetSqlFuncts(
        { "username":"volttron","password":"shines","database":"volttron","hostname":"localhost"},
        {})


if __name__ == '__main__':
    # Entry point for script
    main(sys.argv)
