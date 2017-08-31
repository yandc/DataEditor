#!/user/bin/env python
# coding=utf-8
from config_util import *
from peewee import *
from playhouse.shortcuts import RetryOperationalError

class MyRetryDB(RetryOperationalError, MySQLDatabase):pass

def getOrmModel(dbname, table, columns=[]):
    filename = 'config/db.cfg'
    idx = __file__.rfind('/')
    if idx < 0:
        cf = ConfigUtil(filename)
    else:
        cf = ConfigUtil(__file__[:idx+1]+filename)
    config = {'host':cf.get(dbname, 'host'),
              'port':int(cf.get(dbname, 'port')),
              'user':cf.get(dbname, 'user'),
              'password':cf.get(dbname, 'password')}
    _dbname = cf.get(dbname, 'dbname')
    if len(_dbname) == 0:
        _dbname = dbname
        
    class TableMeta(Model):
        table_schema = CharField(db_column='TABLE_SCHEMA')
        table_name = CharField(db_column='TABLE_NAME')
        data_type = CharField(db_column='DATA_TYPE')
        column_name = CharField(db_column='COLUMN_NAME')
        column_key = CharField(db_column='COLUMN_KEY')
    
        class Meta:
            database = MyRetryDB('information_schema', **config)
            db_table = 'COLUMNS'
            primary_key = CompositeKey('table_schema', 'table_name', 'column_name')

    _fields = TableMeta.raw("select * from information_schema.columns where table_schema='%s' and table_name='%s'"%(_dbname, table)).execute()
    _columns = columns
    class TargetModel(Model):
        for _field in _fields:
            if len(_columns) > 0 and _field.column_name not in _columns:
                continue
            elif _field.data_type == 'int':
                _fieldType = 'Integer'
            elif _field.data_type == 'varchar':
                _fieldType = 'Char'
            elif _field.data_type == 'datetime':
                _fieldType = 'DateTime'
            elif _field.data_type == 'timestamp':
                _fieldType = 'DateTime'
            elif _field.data_type == 'float':
                _fieldType = 'Float'
            elif _field.data_type == 'double':
                _fieldType = 'Double'
            elif _field.data_type == 'decimal':
                _fieldType = 'Decimal'
            elif _field.data_type == 'text':
                _fieldType = 'Text'
            elif _field.data_type == 'bool':
                _fieldType = 'Boolean'
            elif _field.data_type == 'float':
                _fieldType = 'Float'
            elif _field.data_type == 'bigint':
                _fieldType = 'BigInteger'
            elif _field.data_type == 'enum':
                _fieldType = 'Char'

            _index = 'False'
            if _field.column_key == 'PRI':
                _fieldType = 'PrimaryKey'
            elif _field.column_key == 'MUL':
                _index = 'True'
            try:
                exec("%s = %sField(db_column='%s', index=%s)"%(_field.column_name, _fieldType, _field.column_name, _index))
            except:
                logging.error('Build model error: %s'%_field.column_name)
            
        class Meta:
            database = MyRetryDB(_dbname, **config)
            db_table = table

    return TargetModel

if __name__ == '__main__':
    model = getOrmModel('project','base_info')
