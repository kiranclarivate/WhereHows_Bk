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

from wherehows.common.enums import SchedulerType
from wherehows.common import Constant
from com.ziclix.python.sql import zxJDBC
import sys
import FileUtil
from org.slf4j import LoggerFactory


class SchedulerTransform:

  _tables = {"flows": {"columns": "app_id, flow_name,flow_group, flow_path, flow_level, source_version, source_created_time, source_modified_time, wh_etl_exec_id",
                       "file": "flow.csv",
                       "table": "stg_flow"},
             "jobs": {"columns": "app_id, flow_path, source_version, job_name, job_path, job_type, wh_etl_exec_id,additional_info",
                      "file": "job.csv",
                      "table": "stg_flow_job"},
             "dags": {"columns": "app_id, flow_path, source_version, source_job_path, target_job_path, wh_etl_exec_id",
                      "file": "dag.csv",
                      "table": "stg_flow_dag_edge"},
             "owners": {"columns": "app_id, flow_path, owner_id, wh_etl_exec_id",
                        "file": "owner.csv",
                        "table": "stg_flow_owner_permission"},
             "schedules": {"columns": "app_id, flow_path, unit, frequency, cron_expression, effective_start_time, effective_end_time, ref_id, wh_etl_exec_id",
                           "file": "schedule.csv",
                           "table": "stg_flow_schedule"},
             "flow_execs": {"columns": "app_id, flow_name, flow_path, flow_exec_uuid, source_version, flow_exec_status, attempt_id, executed_by, start_time, end_time, wh_etl_exec_id",
                            "file": "flow_exec.csv",
                            "table": "stg_flow_execution"},
             "job_execs": {"columns": "app_id, flow_path, flow_exec_uuid, source_version, job_name, job_path, job_exec_uuid, job_exec_status, attempt_id, start_time, end_time, wh_etl_exec_id",
                           "file": "job_exec.csv",
                           "table": "stg_job_execution"},
             "lineage": {"columns": "app_id, flow_exec_id, job_exec_id,job_exec_uuid,flow_path,job_name,job_start_unixtime,job_finished_unixtime,db_id,abstracted_object_name,full_object_name,partition_start,partition_end,partition_type,storage_type,source_target_type,srl_no,source_srl_no,operation,record_count,insert_count,created_date,wh_etl_exec_id",
                           "file": "lineage.csv",
                           "table": "job_execution_data_lineage"},
             "dataset": {"columns": "db_id,name,schema_type,properties,urn,source,location_prefix,parent_name,storage_type,dataset_type",
                           "file": "dataset.csv",
                           "table": "stg_dict_dataset"}
             }
             

  _read_file_template = """
                        LOAD DATA LOCAL INFILE '{folder}/{file}'
                        INTO TABLE {table}
                        FIELDS TERMINATED BY '\Z' ESCAPED BY '\0'
                        LINES TERMINATED BY '\n'
                        ({columns});
                        """

  _get_flow_id_template = """
                          UPDATE {table} stg
                          JOIN flow_source_id_map fm
                          ON stg.app_id = fm.app_id AND stg.flow_path = fm.source_id_string
                          SET stg.flow_id = fm.flow_id WHERE stg.app_id = {app_id}
                          """

  _get_job_id_template = """
                          UPDATE {table} stg
                          JOIN job_source_id_map jm
                          ON stg.app_id = jm.app_id AND stg.job_path = jm.source_id_string
                          SET stg.job_id = jm.job_id WHERE stg.app_id = {app_id}
                          """

  _clear_staging_tempalte = """
                            DELETE FROM {table} WHERE app_id = {app_id}
                            """

  def __init__(self, args, scheduler_type):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)
    self.app_id = int(args[Constant.JOB_REF_ID_KEY])
    self.db_id = int(args[Constant.APP_DB_ID_KEY])
    self.app_name = args[Constant.APP_NAME]
    self.wh_con = zxJDBC.connect(args[Constant.WH_DB_URL_KEY],
                                 args[Constant.WH_DB_USERNAME_KEY],
                                 args[Constant.WH_DB_PASSWORD_KEY],
                                 args[Constant.WH_DB_DRIVER_KEY])
    temp_dir = FileUtil.etl_temp_dir(args, "CSV")
    self.wh_cursor = self.wh_con.cursor()
    self.app_folder = args[Constant.WH_APP_FOLDER_KEY]
    self.metadata_folder = temp_dir
   

  def run(self):
    try:
      self.read_flow_file_to_stg()
      self.read_job_file_to_stg()
      self.read_dag_file_to_stg()
      self.read_flow_owner_file_to_stg()
      self.read_flow_schedule_file_to_stg()
      self.read_flow_exec_file_to_stg()
      self.read_job_exec_file_to_stg()
      self.read_lineage_file_to_stg()
      self.read_dataset_file_to_stg()
      #self.load_lineage_executions()
    finally:
      self.wh_cursor.close()
      self.wh_con.close()

  def read_flow_file_to_stg(self):
    t = self._tables["flows"]
    str_columns = t.get("columns")
    has_flow_id = False
    if str_columns:
      columns = [x.strip() for x in str_columns.split(',')]
      if 'flow_id' in columns:
        has_flow_id = True

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.info(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    if not has_flow_id:
      # Insert new flow into mapping table to generate flow id
      query = """
            INSERT INTO flow_source_id_map (app_id, source_id_string)
            SELECT sf.app_id, sf.flow_path FROM {table} sf
            WHERE sf.app_id = {app_id}
            AND NOT EXISTS (SELECT * FROM flow_source_id_map where app_id = sf.app_id AND source_id_string = sf.flow_path)
            """.format(table=t.get("table"), app_id=self.app_id)
      print(query)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()

      # Update flow id from mapping table
      query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()

  def read_job_file_to_stg(self):
    t = self._tables["jobs"]
    str_columns = t.get("columns")
    has_flow_id = False
    has_job_id = False
    if str_columns:
      columns = [x.strip() for x in str_columns.split(',')]
      if 'flow_id' in columns:
        has_flow_id = True
      if 'job_id' in columns:
        has_job_id = True

    # Clearing stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    if not has_flow_id:
      query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()

    # ad hoc fix for null values, need better solution by changing the load script
    query = """
            UPDATE {table} stg
            SET stg.ref_flow_path = null
            WHERE stg.ref_flow_path = 'null' and stg.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update sub flow id from mapping table
    if has_flow_id:
      query = """
            UPDATE {table} stg
            JOIN {table} stg1
            ON stg.app_id = stg1.app_id AND stg.ref_flow_path = stg1.flow_path
            SET stg.ref_flow_id = stg1.flow_id WHERE stg.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    else:
        query = """
            UPDATE {table} stg
            JOIN flow_source_id_map fm
            ON stg.app_id = fm.app_id AND stg.ref_flow_path = fm.source_id_string
            SET stg.ref_flow_id = fm.flow_id WHERE stg.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    if not has_job_id:
      # Insert new job into job map to generate job id
      query = """
            INSERT INTO job_source_id_map (app_id, source_id_string)
            SELECT sj.app_id, sj.job_path FROM {table} sj
            WHERE sj.app_id = {app_id}
            AND NOT EXISTS (SELECT * FROM job_source_id_map where app_id = sj.app_id AND source_id_string = sj.job_path)
            """.format(table=t.get("table"), app_id=self.app_id)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()

      # Update job id from mapping table
      query = self._get_job_id_template.format(table=t.get("table"), app_id=self.app_id)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()

    # Update job type id from job type reverse map
    query = """
            UPDATE {table} sj
            JOIN cfg_job_type_reverse_map jtm
            ON sj.job_type = jtm.job_type_actual
            SET sj.job_type_id = jtm.job_type_id
            WHERE sj.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()
    

  def read_dag_file_to_stg(self):
    t = self._tables["dags"]
    str_dag_columns = t.get("columns")
    dag_has_flow_id = False
    if str_dag_columns:
      dag_columns = [x.strip() for x in str_dag_columns.split(',')]
      if 'flow_id' in dag_columns:
        dag_has_flow_id = True

    j = self._tables["jobs"]
    str_job_columns = j.get("columns")
    job_has_flow_id = False
    job_has_job_id = False
    if str_job_columns:
      job_columns = [x.strip() for x in str_job_columns.split(',')]
      if 'flow_id' in job_columns:
        job_has_flow_id = True
      if 'job_id' in job_columns:
        job_has_job_id = True

    # Clearing staging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    if not dag_has_flow_id:
      # Update flow id from mapping table
      query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()

    # Update source_job_id
    if job_has_job_id:
      query = """
            UPDATE {table} sj JOIN {job_table} t ON sj.app_id = t.app_id AND sj.source_job_path = t.job_path
            SET sj.source_job_id = t.job_id
            WHERE sj.app_id = {app_id}
            """.format(app_id=self.app_id, table=t.get("table"), job_table=j.get("table"))
    else:
      query = """
            UPDATE {table} sj JOIN job_source_id_map jm ON sj.app_id = jm.app_id AND sj.source_job_path = jm.source_id_string
            SET sj.source_job_id = jm.job_id
            WHERE sj.app_id = {app_id}
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update target_job_id
    if job_has_job_id:
      query = """
            UPDATE {table} sj JOIN {job_table} t ON sj.app_id = t.app_id AND sj.target_job_path = t.job_path
            SET sj.target_job_id = t.job_id
            WHERE sj.app_id = {app_id}
            """.format(app_id=self.app_id, table=t.get("table"), job_table=j.get("table"))
    else:
      query = """
            UPDATE {table} sj JOIN job_source_id_map jm ON sj.app_id = jm.app_id AND sj.target_job_path = jm.source_id_string
            SET sj.target_job_id = jm.job_id
            WHERE sj.app_id = {app_id}
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update pre_jobs and post_jobs in stg_flow_jobs table
    # need increase group concat max length to avoid overflow
    query = "SET group_concat_max_len=40960"
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj JOIN
            (SELECT source_job_id as job_id, source_version, SUBSTRING(GROUP_CONCAT(distinct target_job_id SEPARATOR ','), 1, 4000) as post_jobs
            FROM {table} WHERE app_id = {app_id} AND source_job_id != target_job_id
            GROUP BY source_job_id, source_version) as d
            ON sj.job_id = d.job_id 
            SET sj.post_jobs = d.post_jobs
            WHERE sj.app_id = {app_id};
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj JOIN
            (SELECT target_job_id as job_id, source_version, SUBSTRING(GROUP_CONCAT(distinct source_job_id SEPARATOR ','), 1, 4000) as pre_jobs
            FROM {table} WHERE app_id = {app_id} AND source_job_id != target_job_id
            GROUP BY target_job_id, source_version) as d
            ON sj.job_id = d.job_id 
            SET sj.pre_jobs = d.pre_jobs
            WHERE sj.app_id = {app_id};
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj
            SET sj.is_first = 'Y'
            WHERE sj.pre_jobs IS NULL AND sj.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj
            SET sj.is_last = 'Y'
            WHERE sj.post_jobs IS NULL AND sj.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = self._clear_staging_tempalte.format(table="stg_flow_dag", app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            INSERT INTO stg_flow_dag (app_id, flow_id, source_version, wh_etl_exec_id, dag_md5)
            SELECT DISTINCT app_id, flow_id, source_version, wh_etl_exec_id, md5(group_concat(source_job_id, "-", target_job_id ORDER BY source_job_id,target_job_id SEPARATOR ",")) dag_md5
            FROM {table} t
            WHERE app_id = {app_id}  and flow_id is not null GROUP BY app_id, flow_id, source_version, wh_etl_exec_id
            UNION
            SELECT DISTINCT app_id, flow_id, source_version, wh_etl_exec_id, 0
            FROM stg_flow sf
            WHERE sf.app_id = {app_id} AND flow_id is not null AND NOT EXISTS (SELECT * FROM {table} t WHERE t.app_id = sf.app_id AND t.flow_id = sf.flow_id AND t.source_version = sf.source_version)
            """.format(app_id=self.app_id, table=t.get("table"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_dag s
            LEFT JOIN flow_dag f
            ON s.app_id = f.app_id AND s.flow_id = f.flow_id AND (f.is_current IS NULL OR f.is_current = 'Y')
            SET s.dag_version = CASE WHEN f.dag_md5 IS NULL THEN 0 WHEN s.dag_md5 != f.dag_md5 THEN f.dag_version + 1 ELSE f.dag_version END
            WHERE s.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_job sj
            JOIN
            (SELECT DISTINCT app_id, flow_id, source_version, dag_version FROM stg_flow_dag) dag
            ON sj.app_id = dag.app_id AND sj.flow_id = dag.flow_id AND sj.source_version = dag.source_version
            SET sj.dag_version = dag.dag_version
            WHERE sj.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)

    self.wh_con.commit()

  def read_flow_owner_file_to_stg(self):
    t = self._tables["owners"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_flow_schedule_file_to_stg(self):
    t = self._tables["schedules"]

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow is_scheduled flag
    query = """
            UPDATE stg_flow f
            LEFT JOIN {table} fs
            ON f.flow_id = fs.flow_id AND f.app_id = fs.app_id
            SET f.is_scheduled = CASE WHEN fs.flow_id IS NULL THEN 'N' ELSE 'Y' END
            WHERE f.app_id = {app_id}
            """.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_flow_exec_file_to_stg(self):
    t = self._tables["flow_execs"]
    str_columns = t.get("columns")
    has_flow_id = False
    if str_columns:
      columns = [x.strip() for x in str_columns.split(',')]
      if 'flow_id' in columns:
        has_flow_id = True

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    if not has_flow_id:
      # Update flow id from mapping table
      query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()
      
     # Insert new flow execution into mapping table to generate flow exec id
    query = """
            INSERT INTO flow_execution_id_map (app_id, source_exec_uuid)
            SELECT sf.app_id, sf.flow_exec_uuid FROM stg_flow_execution sf
            WHERE sf.app_id = {app_id}
            AND NOT EXISTS (SELECT * FROM flow_execution_id_map where app_id = sf.app_id AND source_exec_uuid = sf.flow_exec_uuid)
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_flow_execution stg
            JOIN flow_execution_id_map fm
            ON stg.app_id = fm.app_id AND stg.flow_exec_uuid = fm.source_exec_uuid
            SET stg.flow_exec_id = fm.flow_exec_id WHERE stg.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_job_exec_file_to_stg(self):
    t = self._tables["job_execs"]
    str_columns = t.get("columns")
    has_flow_id = False
    has_job_id = False
    if str_columns:
      columns = [x.strip() for x in str_columns.split(',')]
      if 'flow_id' in columns:
        has_flow_id = True
      if 'job_id' in columns:
        has_job_id = True

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Update flow id from mapping table
    if not has_flow_id:
      query = self._get_flow_id_template.format(table=t.get("table"), app_id=self.app_id)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()

    # Update job id from mapping table
    if not has_job_id:
      query = self._get_job_id_template.format(table=t.get("table"), app_id=self.app_id)
      self.logger.debug(query)
      self.wh_cursor.execute(query)
      self.wh_con.commit()
      
    # Insert new job execution into mapping table to generate job exec id
    query = """
            INSERT INTO job_execution_id_map (app_id, source_exec_uuid)
            SELECT sj.app_id, sj.job_exec_uuid FROM stg_job_execution sj
            WHERE sj.app_id = {app_id}
            AND NOT EXISTS (SELECT * FROM job_execution_id_map where app_id = sj.app_id AND source_exec_uuid = sj.job_exec_uuid)
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_job_execution stg
            JOIN job_execution_id_map jm
            ON stg.app_id = jm.app_id AND stg.job_exec_uuid = jm.source_exec_uuid
            SET stg.job_exec_id = jm.job_exec_id WHERE stg.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            UPDATE stg_job_execution stg
            JOIN flow_execution_id_map fm
            ON stg.app_id = fm.app_id AND stg.flow_exec_uuid = fm.source_exec_uuid
            SET stg.flow_exec_id = fm.flow_exec_id WHERE stg.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

  def read_lineage_file_to_stg(self):
    t = self._tables["lineage"]
    str_columns = t.get("columns")
    has_flow_id = False
    if str_columns:
      columns = [x.strip() for x in str_columns.split(',')]
      if 'flow_id' in columns:
        has_flow_id = True

    # Clear stagging table
    query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()
  def read_dataset_file_to_stg(self):
    t = self._tables["dataset"]
    str_columns = t.get("columns")
    has_flow_id = False
    if str_columns:
      columns = [x.strip() for x in str_columns.split(',')]
      if 'flow_id' in columns:
        has_flow_id = True

    # Clear stagging table
    query = """
             DELETE FROM stg_dict_dataset WHERE db_id = {db_id}
            """.format(db_id=self.db_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()
   # query = self._clear_staging_tempalte.format(table=t.get("table"), app_id=self.app_id)
    #self.logger.debug(query)
    #self.wh_cursor.execute(query)
    #self.wh_con.commit()

    # Load file into stagging table
    query = self._read_file_template.format(folder=self.metadata_folder, file=t.get("file"), table=t.get("table"),
                                            columns=t.get("columns"))
    self.logger.info(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
              INSERT INTO dict_dataset
    ( `name`,
      `schema`,
      schema_type,
      `fields`,
      properties,
      urn,
      `source`,
      location_prefix,
      parent_name,
      storage_type,
      ref_dataset_id,
      is_active,
      dataset_type,
      hive_serdes_class,
      is_partitioned,
      partition_layout_pattern_id,
      sample_partition_full_path,
      source_created_time,
      source_modified_time,
      created_time,
      wh_etl_exec_id
    )
    select s.name, s.schema, s.schema_type, s.fields, s.properties, s.urn,
        s.source, s.location_prefix, s.parent_name,
        s.storage_type, s.ref_dataset_id, s.is_active,
        s.dataset_type, s.hive_serdes_class, s.is_partitioned,
        s.partition_layout_pattern_id, s.sample_partition_full_path,
        s.source_created_time, s.source_modified_time, UNIX_TIMESTAMP(now()),
        s.wh_etl_exec_id
    from stg_dict_dataset s
    where s.db_id = {db_id}
    on duplicate key update
        `name`=s.name, `schema`=s.schema, schema_type=s.schema_type, `fields`=s.fields,
        properties=s.properties, `source`=s.source, location_prefix=s.location_prefix, parent_name=s.parent_name,
        storage_type=s.storage_type, ref_dataset_id=s.ref_dataset_id, is_active=s.is_active,
        dataset_type=s.dataset_type, hive_serdes_class=s.hive_serdes_class, is_partitioned=s.is_partitioned,
        partition_layout_pattern_id=s.partition_layout_pattern_id, sample_partition_full_path=s.sample_partition_full_path,
        source_created_time=s.source_created_time, source_modified_time=s.source_modified_time,
        modified_time=UNIX_TIMESTAMP(now()), wh_etl_exec_id=s.wh_etl_exec_id;
            """.format(db_id=self.db_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    query = """
            update job_execution_data_lineage jedl
            join job_execution je on je.app_id = jedl.app_id and je.job_exec_uuid = jedl.job_exec_uuid and je.job_name= jedl.job_name
              set jedl.flow_exec_id = je.flow_exec_id where je.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()
    query = """
            update job_execution_data_lineage jedl
            join job_execution je on je.app_id = jedl.app_id and je.job_exec_uuid = jedl.job_exec_uuid and je.job_name= jedl.job_name
              set jedl.job_exec_id = je.job_exec_id where je.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()
    query = """
            update job_execution_data_lineage jedl
            set jedl.job_finished_unixtime = unix_timestamp(NOW()) where jedl.app_id = {app_id}
            """.format(app_id=self.app_id)
    self.logger.debug(query)
    self.wh_cursor.execute(query)
    self.wh_con.commit()

    


  def load_lineage_executions(self):
    cmd = """INSERT INTO job_execution_data_lineage
              (app_id, flow_exec_id, job_exec_id,job_exec_uuid,flow_path,job_name,job_start_unixtime,job_finished_unixtime,db_id,abstracted_object_name,full_object_name,partition_start,partition_end,partition_type,storage_type,source_target_type,srl_no,source_srl_no,operation,record_count,insert_count,created_date,wh_etl_exec_id)
              SELECT app_id, flow_exec_id, job_exec_id, flow_path, job_name,
               unix_timestamp(NOW()),unix_timestamp(NOW()),db_id, abstracted_object_name,full_object_name,partition_start,partition_end,partition_type,storage_type,source_target_type,srl_no,source_srl_no,operation,record_count,insert_count,created_date,wh_etl_exec_id
               FROM stg_job_execution_data_lineage s
               WHERE s.app_id = {app_id} 
               ON DUPLICATE KEY UPDATE
               flow_exec_id = s.flow_exec_id,
               job_exec_id = s.job_exec_id,
               job_exec_uuid = s.job_exec_uuid
               flow_path = s.flow_path,
               job_name = s.job_name,
               job_start_unixtime = unix_timestamp(NOW()),
               job_finished_unixtime = unix_timestamp(NOW()),
               db_id = s.db_id,
               abstracted_object_name = s.abstracted_object_name,
               full_object_name = s.full_object_name,
               partition_start = s.partition_start,
               partition_end = s.partition_end,
               partition_type = s.partition_type,
               storage_type = s.storage_type,
               source_target_type = s.source_target_type,
               srl_no = s.srl_no,
               source_srl_no = s.source_srl_no,
               operation = s.operation,
               record_count = s.record_count,
               insert_count = s.insert_count,
               created_date = s.created_date,
               wh_etl_exec_id = s.wh_etl_exec_id
               """.format(app_id=self.app_id)
    self.logger.debug(cmd)
    self.wh_cursor.execute(cmd)
    self.wh_con.commit()  


if __name__ == "__main__":
  props = sys.argv[1]
  st = SchedulerTransform(props, SchedulerType.GENERIC)
  st.run()

