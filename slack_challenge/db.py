import os
import yaml
import pymysql
from pymysql.err import InternalError,IntegrityError
from dateutil.parser import parse

class DB(object):
    def __init__(self):
        self.config = os.getcwd() + "/config.yaml"
        self.config_yaml = self._get_config()

        self.schema = self.config_yaml.get('sql_config').get(
                'schema')
        self.dbhost = self.config_yaml.get('sql_config').get('host')
        print "Hostname: "+self.dbhost
        self.dbuser = self.config_yaml.get('sql_config').get('user')
        self.dbpass = self.config_yaml.get('sql_config').get('pass')
        print "PWD: "+self.dbpass
        self.dbport = self.config_yaml.get('sql_config').get('port')
        self.conn = pymysql.connect(host=self.dbhost,
                                        port=self.dbport,
                                        user=self.dbuser,
                                        passwd=self.dbpass,
                                        db=self.schema, charset='utf8',
                                        autocommit=True)
        self.cursor = self.conn.cursor()

    def _get_config(self):
        return yaml.load(open(self.config))

    def create_tables(self):
        for name, ddl in self.config_yaml.get('ddl').items():
            if 'drop' in name:
                print("Removing table {}: ".format(name))
            elif 'create' in name:
                print("Creating table {}: ".format(name))
            try:
                self.cursor.execute(ddl)
            except InternalError as e:
                print e

    def insert(self, table=None, rows=None):
        if rows is None or table is None:
            print "Either no data or table name is missing."
            return False
        if table is 'email':
            for r in rows:
                try:
                    self.cursor.execute("INSERT INTO email(message_id, sender, subject, email_date, label, sub_md5) VALUES (%s,%s,%s,%s,%s, %s)",r)
                except IntegrityError as e:
                    print "Could not insert row (%s,%s,%s,%s,%s,%s) to email table" %r
                    print e

        elif table is 'recipient':
            for r in rows:
                try:
                    self.cursor.execute("INSERT INTO recipient(message_id, sender, recipient, subject, sub_md5, email_date, is_to, is_cc, is_bcc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",r)
                except IntegrityError as e:
                    print "Could not insert row (%s,%s,%s,%s,%s,%s,%s,%s,%s) to recipient table" %r
                    print e
        else:
            print "This table is not yet setup for this project"
            return False

    def run_query(self, query=None, n=None):
        if query is None:
            return None
        self.cursor.execute(query)
        if n is None:
            return self.cursor.fetchall()
        elif n == 1:
            return self.cursor.fetchone()
        else:
            return self.cursor.fetchmany(5)

    def convert_date_format(self,date=None):
        if date:
            return parse(date).strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None
