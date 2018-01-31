#
# Copyright 2015 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#

import csv
import datetime
import FileUtil
import json
import os
import sys

from com.ziclix.python.sql import zxJDBC
from org.slf4j import LoggerFactory
from wherehows.common import Constant


class GPExtract:
  table_dict = {}
  table_output_list = []
  field_output_list = []
  sample_output_list = []

  ignored_owner_regex = 'ANONYMOUS|PUBLIC|SYS|SYSTEM|DBSNMP|MDSYS|CTXSYS|XDB|TSMSYS|GREENPLUM.*|APEX.*|TEST?*|GG_.*|\$'

  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def get_table_info(self, excluded_owner_list,table_name,included_schema_list):
    """
    get table, column info from GreenPlum all_tables
    '''
    here Owner, Schema, Database have same meaning: a collection of tables
    :param excluded_owner_list: schema blacklist
    :param table_name: get specific table name, not used in common case
    :return:
    """
    owner_exclusion_filter = ''
    table_name_filter = ''
    schema_included_filter = ''
    if excluded_owner_list and len(excluded_owner_list) > 0:
      #owner_exclusion_filter = " AND NOT REGEXP_LIKE(t.OWNER, '%s') " % '|'.join(excluded_owner_list)
      owner_exclusion_filter = " AND pt.tableowner not  like  '%s%%' " % '|'.join(excluded_owner_list)
      #owner_exclusion_filter = owner_exclusion_filter.join("%%")
      self.logger.info("Get GreenPlum metadata with extra excluded schema: %s" % owner_exclusion_filter)
    if included_schema_list and len(included_schema_list) > 0:
      schema_included_filter = " pgns.nspname similar to  ('%%(%s)%%')   " % included_schema_list
      self.logger.info("Get GreenPlum metadata with extra included  schema: %s" % schema_included_filter)
    #if table_name and len(table_name) > 0:
      #self.logger.info("Get GreenPlum metadata with extra filter: %s" % table_name_filter)
     # if table_name.find('.') > 0:
      #  table_name_filter = " AND OWNER='%s' AND TABLE_NAME='%s' " % table_name.split('.')
      #else:
       # table_name_filter = " AND TABLE_NAME='%s' " % table_name
      #self.logger.info("Get GreenPlum metadata with extra filter: %s" % table_name_filter)

    curs_meta = self.conn_db.cursor()
    self.logger.info("Get GreenPlum metadata with extra included  schema33333: %s %s" % (schema_included_filter, owner_exclusion_filter))
    self.logger.info("Get GreenPlum metadata with extra excluded schema22222: %s" % owner_exclusion_filter)

    column_info_sql = "  SELECT pgns.nspname,pt.tablename,'N' as PARTITIONED,c.ORDINAL_POSITION,c.COLUMN_NAME,c.DATA_TYPE,c.IS_NULLABLE,c.CHARACTER_MAXIMUM_LENGTH,c.NUMERIC_PRECISION,c.NUMERIC_SCALE,c.CHARACTER_OCTET_LENGTH,c.CHARACTER_SET_CATALOG,c.CHARACTER_SET_NAME,c.CHARACTER_SET_SCHEMA,c.COLUMN_DEFAULT  ,pd.description FROM pg_catalog.pg_namespace pgns     LEFT JOIN pg_tables pt on pt.schemaname = pgns.nspname    LEFT JOIN INFORMATION_SCHEMA.TABLES t  on pt.tablename= t.TABLE_SCHEMA    LEFT JOIN INFORMATION_SCHEMA.COLUMNS c on c.TABLE_SCHEMA = t.TABLE_SCHEMA and c.TABLE_NAME = t.TABLE_NAME    LEFT JOIN pg_class pc on pc.relname=t.TABLE_NAME     LEFT JOIN pg_description pd on pd.objoid=pc.oid     LEFT JOIN pg_attribute pa on pa.attrelid=pc.oid AND pa.attnum = pd.objsubid AND pa.attname=c.COLUMN_NAME    where %s %s ORDER BY 1   ,pt.tablename like '%%readme%%' desc,    pt.tablename asc,4;" % (schema_included_filter, owner_exclusion_filter)   
    self.logger.info(column_info_sql)
    curs_meta.execute(column_info_sql)

    rows = curs_meta.fetchall() 
    self.logger.info("Fetched %d records of GreenPlum metadata" % len(rows))
    curs_meta.close()

    prev_table_key = ''
    for row in rows:
      current_table_key = "%s.%s" % (row[0], row[1])  # OWNER.TABLE_NAME
      if current_table_key != prev_table_key:
        self.table_dict[current_table_key] = {"partitioned": row[2]}
        prev_table_key = current_table_key

    self.logger.info("Fetched %d tables: %s" % (len(self.table_dict), self.table_dict))
    return rows


  def get_extra_table_info(self):
    """
    Index, Partition, Size info
    :return: index,partition,constraint
    """
    index_info_sql = """
    select  tc.TABLE_SCHEMA,tc.TABLE_NAME,i.relname as INDEX_NAME,ix.indisunique as ISUNIQUE,tc.CONSTRAINT_NAME, array_to_string(array_agg(a.attname), ', ') as COLUMN_NAMES
from pg_class t,  pg_class i, pg_index ix, pg_attribute a ,information_schema.table_constraints tc where t.oid = ix.indrelid   and i.oid = ix.indexrelid  and a.attrelid = t.oid
and a.attnum = ANY(ix.indkey) and t.relkind = 'r'  and tc.table_name=t.relname  
group by  t.relname, i.relname,ix.indisunique,tc.constraint_name,tc.table_schema,tc.table_name order by
t.relname, i.relname;"""

    partition_col_sql = """
     select * from pg_catalog.pg_inherits;"""

    curs_meta = self.conn_db.cursor()

    # get index and partition info one by one

    curs_meta.execute(partition_col_sql)
    rows = curs_meta.fetchall()
    for row in rows:
      table_name_key = "%s.%s" % (row[0], row[1])
      if table_name_key not in self.table_dict:
        continue

      self.table_dict[table_name_key]['partition_columns'] = row[2]
    self.logger.info("Found %d record for partition info" % len(rows))

    curs_meta.execute(index_info_sql)
    rows = curs_meta.fetchall()
    curs_meta.close()
    for row in rows:
      table_name_key = "%s.%s" % (row[0], row[1])
      if table_name_key not in self.table_dict:
        continue

      if "indexes" not in self.table_dict[table_name_key]:
        self.table_dict[table_name_key]["indexes"] = []

      self.table_dict[table_name_key]["indexes"].append(
        {
          "name": row[2],
         # "type": row[3],
          "is_unique": 'Y' if row[3] == 'UNIQUE' else 'N',
          "constraint_name": row[4],
          "index_columns": row[5],
          #"num_of_columns": row[7]
        }
      )
    self.logger.info("Found %d record for index info" % len(rows))



  def format_table_metadata(self, rows):
    """
    add table info with columns from rows into table schema
    :param rows: input. each row is a table column
    :param schema: {schema : _, type : _, tables : ['name' : _, ... 'original_name' : _] }
    :return:
    """
    schema_dict = {"fields": []}
    table_record = {}
    table_idx = 0
    field_idx = 0
    

    for row in rows:
      table_name_key = "%s.%s" % (row[0], row[1])
      table_urn = "greenplum:///%s/%s" % (row[0], row[1])

      if 'urn' not in table_record or table_urn != table_record['urn']:
        # This is a new table. Let's push the previous table record into output_list
        if 'urn' in table_record:
          schema_dict["num_fields"] = field_idx
          table_record["columns"] = json.dumps(schema_dict)
          self.table_output_list.append(table_record)

        properties = {
          "indexes": self.table_dict[table_name_key].get("indexes"),
          "partition_column": self.table_dict[table_name_key].get("partition_column")
        }
        table_record = {
          "name": row[1],
          "columns": None,
          "schema_type": "JSON",
          "properties": json.dumps(properties),
          "urn": table_urn,
          "source": "GreenPlum",
          "location_prefix": row[0],
          "parent_name": row[0],
          "storage_type": "Table",
          "dataset_type": "greenplum",
          "is_partitioned": 'Y' if self.table_dict[table_name_key]["partitioned"] == 'YES' else 'N'
        }
        schema_dict = {"fields": []}
        table_idx += 1
        field_idx = 0

      field_record = {
        "sort_id": self.num_to_int(row[3]),
        "name": row[4],
        "data_type": row[5],
        "nullable": row[6],
        "size": self.num_to_int(row[7]),
        "precision": self.num_to_int(row[8]),
        "scale": self.num_to_int(row[9]),
        "default_value": self.trim_newline(row[14]),
        "doc": self.trim_newline(row[15])
      }
      schema_dict['fields'].append(field_record)
      field_record['dataset_urn'] = table_urn
      self.field_output_list.append(field_record)
      field_idx += 1

    # finish all remaining rows
    schema_dict["num_fields"] = field_idx
    table_record["columns"] = json.dumps(schema_dict)
    self.table_output_list.append(table_record)
    self.logger.info("%d Table records generated" % table_idx)


  def get_sample_data(self, table_fullname, num_rows):
    """
    select top rows from table as sample data
    :return: json of sample data
    """
    table_urn = "greenplum:///%s" % (table_fullname.replace('.', '/'))
    columns = []
    sample_data = []

    sql = 'SELECT * FROM %s LIMIT  %d' % (table_fullname, num_rows)
    cursor = self.conn_db.cursor()
    try:
      cursor.execute(sql)
      rows = cursor.fetchall()
      if len(rows) == 0:
        self.logger.error("dataset {} is empty".format(table_fullname))
        return

      # retrieve column names
      columns = [i[0] for i in cursor.description]
      # self.logger.info("Table {} columns: {}".format(table_fullname, columns))

      # retrieve data
      for row in rows:
        row_data = []
        # encode each field to a new value
        for value in row:
          if value is None:
            row_data.append('')
          else:
            row_data.append(unicode(value, errors='ignore'))
        sample_data.append(row_data)
    except Exception as ex:
      self.logger.error("Error fetch sample for {}: {}".format(table_fullname, str(ex)))
      return

    cursor.close()
    data_with_column = map(lambda x: dict(zip(columns, x)), sample_data)
    self.sample_output_list.append({'dataset_urn': table_urn, 'sample_data': data_with_column})


  def num_to_int(self, num):
    try:
      return int(num)
    except (ValueError, TypeError):
      return None

  def trim_newline(self, line):
    return line.replace('\n', ' ').replace('\r', ' ').strip().encode('ascii', 'ignore') if line else None

  def write_csv(self, csv_filename, csv_columns, data_list):
    csvfile = open(csv_filename, 'wb')
    os.chmod(csv_filename, 0644)
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns, delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\0')
    writer.writeheader()
    for data in data_list:
      writer.writerow(data)
    csvfile.close()


  def run(self, exclude_database_list, include_schema_list, table_name, table_file, field_file, sample_file, sample=False):
    """
    The entrance of the class, extract schema and sample data
    Notice the database need to have a order that the databases have more info (DWH_STG) should be scaned first.
    :param exclude_database_list: list of excluded databases/owners/schemas
    :param include_schema_list: list of included schemas
    :param table_name: specific table name to query
    :param table_file: table output csv file path
    :param field_file: table fields output csv file path
    :param sample_file: sample data output csv file path
    :param sample: do sample or not
    :return:
    """
    begin = datetime.datetime.now().strftime("%H:%M:%S")
    # collect table info
    rows = self.get_table_info(exclude_database_list, table_name,include_schema_list)
    self.get_extra_table_info()
    self.format_table_metadata(rows)
    mid = datetime.datetime.now().strftime("%H:%M:%S")
    self.logger.info("Collecting table info [%s -> %s]" % (str(begin), str(mid)))

    csv_columns = ['name', 'columns', 'schema_type', 'properties', 'urn', 'source', 'location_prefix',
                   'parent_name', 'storage_type', 'dataset_type', 'is_partitioned']
    self.write_csv(table_file, csv_columns, self.table_output_list)

    csv_columns = ['dataset_urn', 'sort_id', 'name', 'data_type', 'nullable',
                   'size', 'precision', 'scale', 'default_value', 'doc']
    self.write_csv(field_file, csv_columns, self.field_output_list)

    if sample:
      # collect sample data
      for table in self.table_dict.keys():
        self.get_sample_data(table, 10)
      end = datetime.datetime.now().strftime("%H:%M:%S")
      self.logger.info("Collecting sample data [%s -> %s]" % (str(mid), str(end)))

      csv_columns = ['dataset_urn', 'sample_data']
      self.write_csv(sample_file, csv_columns, self.sample_output_list)
          
    

if __name__ == "__main__":
  args = sys.argv[1]

  # connection
  username = args[Constant.GP_DB_USERNAME_KEY]
  password = args[Constant.GP_DB_PASSWORD_KEY]
  JDBC_DRIVER = args[Constant.GP_DB_DRIVER_KEY]
  JDBC_URL = args[Constant.GP_DB_URL_KEY]

  e = GPExtract()
  e.conn_db = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
  exclude_databases = filter(bool, args[Constant.GP_EXCLUDE_DATABASES_KEY].split(','))
  include_schemas = args[Constant.GP_INCLUDE_SCHEMAS_KEY]
  collect_sample = False
  if Constant.GP_LOAD_SAMPLE in args:
    collect_sample = bool(args[Constant.GP_LOAD_SAMPLE])
  temp_dir = FileUtil.etl_temp_dir(args, "GREENPLUM")
  table_output_file = os.path.join(temp_dir, args[Constant.GP_SCHEMA_OUTPUT_KEY])
  field_output_file = os.path.join(temp_dir, args[Constant.GP_FIELD_OUTPUT_KEY])
  sample_output_file = os.path.join(temp_dir, args[Constant.GP_SAMPLE_OUTPUT_KEY])

  try:
    #e.conn_db.cursor().execute("ALTER SESSION SET TIME_ZONE = 'US/Pacific'")
    #e.conn_db.cursor().execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
    #e.conn_db.cursor().execute("CALL dbms_application_info.set_module('%s','%d')" %
     #                          ('WhereHows (Jython)', os.getpid()))
    #e.conn_db.commit()

    e.run(exclude_databases,
          include_schemas,
          None,
          table_output_file,
          field_output_file,
          sample_output_file,
          sample=collect_sample)
  finally:
    e.conn_db.cursor().close()
    e.conn_db.close()
