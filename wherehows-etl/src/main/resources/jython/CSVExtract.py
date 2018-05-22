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


class CSVExtract:
  table_dict = {}
  table_output_list = []
  field_output_list = []
  sample_output_list = []

 

  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def get_csv_Data(self,csv_input_file,flow_output_file,job_output_file, dag_output_file, owner_output_file,schedule_output_file,flow_exec_output_file,job_exec_output_file,lineage_output_file,dataset_output_file):
    #folder,csv_path = os.path.split(csv_input_file)
    csv_path=csv_input_file
   
    app_name = "DFPLUS"
    app_id = "103"
    
    
    flowHeaders = ["app_id", "flow_name","flow_group", "flow_path", "flow_level", "source_version", "source_created_time", "source_modified_time", "wh_etl_exec_id"]
    jobHeaders = ["app_id", "flow_path", "source_version", "job_name", "job_path", "job_type", "wh_etl_exec_id","AdditionalInfo"]
    dagHeaders = ["app_id", "flow_path", "source_version", "source_job_path", "target_job_path", "wh_etl_exec_id"]
    ownerHeaders = ["app_id", "flow_path", "owner_id", "wh_etl_exec_id"]
    scheduleHeaders = ["app_id", "flow_path", "unit", "frequency", "cron_expression", "effective_start_time", "effective_end_time", "ref_id", "wh_etl_exec_id"]
    flowExecHeaders = ["app_id", "flow_name", "flow_path", "flow_exec_uuid", "source_version", "flow_exec_status", "attempt_id", "executed_by", "'start_time", "end_time", "wh_etl_exec_id"]
    jobExecHeaders = ["app_id", "flow_path", "flow_exec_uuid", "source_version", "job_name", "job_path", "job_exec_uuid", "job_exec_status", "attempt_id", "start_time", "end_time", "wh_etl_exec_id"]
    lineageHeaders = ["app_id","flow_exec_id","job_exec_id","job_exec_uuid","flow_path","job_name","job_start_unixtime","job_finished_unixtime","db_id","abstracted_object_name","full_object_name","partition_start","partition_end","partition_type","storage_type","source_target_type","srl_no","source_srl_no","operation","record_count","insert_count","created_date","wh_etl_exec_id"]
    datasetHeaders = ["db_id","name","schema_type","properties","urn","source","location_prefix","parent_name","storage_type","dataset_type"]
    temp_dir = FileUtil.etl_temp_dir(args, "CSV")
    
    #**************** Writing flow.csv **********************#
    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)
        done = False
        
        with open(flow_output_file,'w')  as csvfile:
          writer = csv.DictWriter(csvfile,fieldnames=flowHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
           
          #writer.writeheader()
          rowDict = {}
          for row in csvreader:
            
            mydict = dict(zip(fields,row)) 
            rowDict["app_id"] = "103"

            
               
            for field in fields:
                    if "flow_name" in rowDict and  rowDict["flow_name"] == mydict[field]:                        
                        done = True
                        break
              
                    if field == 'flow_name / schedule name' and mydict[field]:
                        rowDict["flow_name"] = mydict[field]
                        rowDict["flow_group"] = app_name
                    #if field == 'Job Name':
                        flow_path = app_name+":"+mydict["flow_name / schedule name"]
                        rowDict["flow_path"] = flow_path
                        done = False
            if not done:
              writer.writerow(rowDict)
   #**************** Writing job.csv **********************#
    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)
        
        with open(job_output_file,'w') as jobcsvfile:
          writer = csv.DictWriter(jobcsvfile,fieldnames=jobHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
          #writer.writeheader()
          rowDict = {}
        
          jsonDict = {}
          for row in csvreader:
            
            mydict = dict(zip(fields,row))
            rowDict["app_id"] = "103"
            for field in fields:
              if field == 'flow_name / schedule name' and mydict[field]:
                                 
                flow_path = app_name+":"+mydict[field]
                rowDict["flow_path"] = flow_path
              
              if field == 'Job Name' and mydict["flow_name / schedule name"]:
                job_path = app_name+":"+mydict["flow_name / schedule name"]+"/"+mydict[field]
                rowDict["job_name"] = mydict[field]
                rowDict["job_path"] = job_path
              elif field not in( 'flow_name / schedule name','Job Name') and mydict["flow_name / schedule name"]:
                jsonDict[field] = mydict[field]
                rowDict["AdditionalInfo"] = json.dumps(jsonDict)
            
            
               
            

               
        
            writer.writerow(rowDict)

    #**************** Writing dag.csv **********************#
    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)

        with open(dag_output_file,'w') as jobcsvfile:
            writer = csv.DictWriter(jobcsvfile,fieldnames=dagHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
            #writer.writeheader()
            rowDict = {}
            number = 0
            flowSet = {}
            flowDict = {}
            #flowSet = set()
            
            for row in csvreader:
                #print(row)
                mydict = dict(zip(fields,row))
                rowDict["app_id"] = "103"
                rowDict["flow_path"] = ''
                rowDict["source_job_path"] = ''
                rowDict["target_job_path"] = ''
                
                
                for field in fields:
                    if field == 'flow_name / schedule name' and  mydict[field]:

                        #flowDict[mydict[field]]=number
                        #flowDict['count']=number

                        
                        if mydict[field] in flowDict:
                          flowDict[mydict[field]] += 1
                          number = flowDict[mydict[field]]
                          
                        else:
                          flowDict[mydict[field]]=0
                          number = 0
                                               
                        flow_path = app_name+":"+mydict["flow_name / schedule name"]
                        rowDict["flow_path"] = flow_path
                        
                        
                        
                          
                         
                        #print(mydict['Dependencies'])
                    if field == 'Dependencies' and mydict['flow_name / schedule name'] and mydict[field]:
                        flow_path = app_name+":"+mydict["flow_name / schedule name"]
                        source_job_path = app_name+":"+mydict['flow_name / schedule name']+"/"+mydict[field]
                        target_job_path = app_name+":"+mydict['flow_name / schedule name']+"/"+mydict["Job Name"]
                        rowDict["source_job_path"] = source_job_path
                        rowDict["target_job_path"] = target_job_path
                        rowDict["source_version"] = number
                #print(rowDict["source_job_path"])
                        
                    
                    
                writer.writerow(rowDict)

     #**************** Writing owner.csv **********************#

    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)

        with open(owner_output_file,'w') as jobcsvfile:
            writer = csv.DictWriter(jobcsvfile,fieldnames=ownerHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
            #writer.writeheader()
            rowDict = {}
            
            for row in csvreader:
                
                mydict = dict(zip(fields,row))
                rowDict["app_id"] = "103"
                for field in fields:
                    if field == 'flow_name / schedule name' and mydict[field]:
                        
                        flow_path = app_name+":"+mydict["flow_name / schedule name"]
                        rowDict["flow_path"] = flow_path
                    if field == 'assigned userid':
                        rowDict["owner_id"] = mydict[field]
                        
                    
                writer.writerow(rowDict)
                
    
    #**************** Writing schedule.csv **********************#
    

    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)
        with open(schedule_output_file,'w') as jobcsvfile:
            writer = csv.DictWriter(jobcsvfile,fieldnames=scheduleHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
            #writer.writeheader()
            rowDict = {}
            
            for row in csvreader:
                
                mydict = dict(zip(fields,row))
                rowDict["app_id"] = "103"
                for field in fields:
                    if field == 'flow_name / schedule name' and mydict[field]:
                        
                        flow_path = app_name+":"+mydict["flow_name / schedule name"]
                        rowDict["flow_path"] = flow_path
                    
                        
                    
                writer.writerow(rowDict)


            #**************** Writing flow_exec.csv **********************#
    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)

        with open(flow_exec_output_file,'w') as jobcsvfile:
            writer = csv.DictWriter(jobcsvfile,fieldnames=flowExecHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
            #writer.writeheader()
            rowDict = {}
            
            for row in csvreader:
                
                mydict = dict(zip(fields,row))
                rowDict["app_id"] = "103"
                for field in fields:
                    if field == 'flow_name / schedule name' and mydict[field]:
                        rowDict["flow_name"] = mydict[field]
                        flow_path = app_name+":"+mydict[field]
                        rowDict["flow_path"] = flow_path
                        rowDict["flow_exec_uuid"] = "000000-csv"+app_id+mydict[field]
                    if field == "assigned userid" and mydict["flow_name / schedule name"]:
                        rowDict["executed_by"] = mydict[field]     
                writer.writerow(rowDict)  

       
    #**************** Writing job_exec.csv **********************#
    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)           
  

        with open(job_exec_output_file,'w') as jobcsvfile:
            writer = csv.DictWriter(jobcsvfile,fieldnames=jobExecHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
            #writer.writeheader()
            rowDict = {}
            flow_exec_id = 0
            job_exec_id = 0
            for row in csvreader:
                flow_exec_id +=1
                job_exec_id +=1
                
                mydict = dict(zip(fields,row))
                rowDict["app_id"] = "103"
                for field in fields:
                    if field == 'flow_name / schedule name' and mydict[field]:
                        flow_path = app_name+":"+mydict[field]
                        rowDict["flow_path"] = flow_path
                        rowDict["flow_exec_uuid"] = "000000-csv"+app_id+mydict[field]
                    if field == "Job Name":
                        job_path = app_name+":"+mydict["flow_name / schedule name"]+"/"+mydict[field]
                        rowDict["job_name"] = mydict[field]
                        rowDict["job_path"] = job_path
                        rowDict["job_exec_status"] = "SUCESS"
                        rowDict["job_exec_uuid"] = "000000-csv"+app_id+mydict[field]
                    
                writer.writerow(rowDict)


    #**************** Writing lineage.csv **********************#
    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)           
  

        with open(lineage_output_file,'w') as jobcsvfile:
            writer = csv.DictWriter(jobcsvfile,fieldnames=lineageHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
            #writer.writeheader()
            rowDict = {}
            source = "file:///"
            storage_type = "Flat File"
            flow_exec_id = 0
            job_exec_id = 0
            app_id = "103"
            
            for row in csvreader:
                
                mydict = dict(zip(fields,row))
                rowDict["app_id"] = "103"
                #print(mydict['Inputs'])
                inputsSet = mydict['Inputs'].splitlines()
                outputsSet = mydict['Outputs'].splitlines()
                srl_no = 0
                flow_exec_id +=1
                job_exec_id +=1
                
                flow_path = app_name+":"+mydict['flow_name / schedule name']
                job_path = app_name+":"+mydict["flow_name / schedule name"]+"/"+mydict['Job Name']
                
                for input in inputsSet:
                  srl_no +=1                  
                  operation = 'read'
                  source_target_type = 'source'
                  input = input.replace('"','')
                  if input.startswith("/"):
                    abstracted_object_name = "/"+app_name+input
                  else:
                    abstracted_object_name = "/"+app_name+"/"+input
                            
                  
                  full_object_name = source + abstracted_object_name
                  rowDict["flow_path"] = flow_path
                  rowDict["job_name"] = mydict['Job Name']
                  rowDict["job_start_unixtime"] = '1440000000'
                  rowDict["job_finished_unixtime"] = '1524836800'
                  rowDict["db_id"] = '10003'
                  rowDict["abstracted_object_name"] = abstracted_object_name
                  rowDict["full_object_name"] = full_object_name
                  rowDict["storage_type"] = storage_type
                  rowDict["source_target_type"] = source_target_type
                  rowDict["srl_no"] = srl_no
                  rowDict["operation"] = operation
                  rowDict["flow_exec_id"] = flow_exec_id
                  rowDict["job_exec_id"] = job_exec_id
                  rowDict["job_exec_uuid"] = "000000-csv"+app_id+mydict['Job Name']
                  writer.writerow(rowDict)

                for output in outputsSet:
                  srl_no +=1                  
                  operation = 'write'                  
                  source_target_type = 'target'
                  output = output.replace('"','')
                  if output.startswith("/"):
                    abstracted_object_name = "/"+app_name+output
                  else:
                    abstracted_object_name = "/"+app_name+"/"+output
                  full_object_name = source + abstracted_object_name
                  rowDict["flow_path"] = flow_path
                  rowDict["job_name"] = mydict['Job Name']
                  rowDict["job_start_unixtime"] = '1440000000'
                  rowDict["job_finished_unixtime"] = '1524836800'
                  rowDict["db_id"] = '10003'
                  rowDict["abstracted_object_name"] = abstracted_object_name
                  rowDict["full_object_name"] = full_object_name
                  rowDict["storage_type"] = storage_type
                  rowDict["source_target_type"] = source_target_type
                  rowDict["srl_no"] = srl_no
                  rowDict["operation"] = operation
                  rowDict["flow_exec_id"] = flow_exec_id
                  rowDict["job_exec_id"] = job_exec_id
                  rowDict["job_exec_uuid"] = "000000-csv"+app_id+mydict['Job Name']
                  writer.writerow(rowDict)
                  
                  
                  
                
                
                        
                    
                    
    
    #**************** Writing dataset.csv **********************#
    with open(csv_path,"r") as f_obj:
        #csv_reader(f_obj)

        csvreader = csv.reader(f_obj)
        fields = next(csvreader)           
  

        with open(dataset_output_file,'w') as jobcsvfile:
            writer = csv.DictWriter(jobcsvfile,fieldnames=datasetHeaders,delimiter='\x1A', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, quotechar='\1', escapechar='\\')
            #writer.writeheader()
            rowDict = {}
            source = "file:///DFPLUS"
            storage_type = "Flat File"
            flow_exec_id = 0
            job_exec_id = 0
            
            for row in csvreader:
                
                mydict = dict(zip(fields,row))
                #rowDict["app_id"] = "103"
                #print(mydict['Inputs'])
                inputsSet = mydict['Inputs'].splitlines()
                outputsSet = mydict['Outputs'].splitlines()
                srl_no = 0
                flow_exec_id +=1
                job_exec_id +=1
                
                flow_path = app_name+":"+mydict['flow_name / schedule name']
                job_path = app_name+":"+mydict["flow_name / schedule name"]+"/"+mydict['Job Name']
                
                for input in inputsSet:
                  srl_no +=1                  
                  operation = 'read'
                  source_target_type = 'source'
                  input = input.replace('"','')
                  rowDict["db_id"] = '10003'
                  rowDict["name"] = os.path.split(os.path.abspath(input))[1]
                  #rowDict["schema"] = ''
                  rowDict["schema_type"] = 'JSON'
                  rowDict["properties"] = ''
                  if input.startswith("/"):
                    rowDict["urn"] = source + input
                  else:
                    rowDict["urn"] = source +"/"+ input                  
                  rowDict["source"] = 'file'
                  #rowDict["urn"] = source + input
                  rowDict["location_prefix"] = 'DFPLUS'
                  rowDict["parent_name"] = 'DFPLUS'
                  rowDict["storage_type"] = 'Flat File'
                  rowDict["dataset_type"] = 'file'
                  writer.writerow(rowDict)
                for output in outputsSet:
                  srl_no +=1                  
                  operation = 'read'
                  source_target_type = 'source'
                  output = output.replace('"','')
                  rowDict["db_id"] = '10003'
                  rowDict["name"] = os.path.split(os.path.abspath(output))[1]
                  #rowDict["schema"] = ''
                  rowDict["schema_type"] = 'JSON'
                  rowDict["properties"] = ''
                  if output.startswith("/"):
                    rowDict["urn"] = source + output
                  else:
                    rowDict["urn"] = source +"/"+ output  
                  rowDict["source"] = 'file'
                  #rowDict["urn"] = source + input
                  rowDict["location_prefix"] = 'DFPLUS'
                  rowDict["parent_name"] = 'DFPLUS'
                  rowDict["storage_type"] = 'Flat File'
                  rowDict["dataset_type"] = 'file'
                  writer.writerow(rowDict)
                  

                


  def run(self,csv_input_file,flow_output_file,job_output_file,dag_output_file,owner_output_file,schedule_output_file,flow_exec_output_file,job_exec_output_file,lineage_output_file,dataset_output_file):
    
    """
    The entrance of the class, extract schema and sample data
 
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
    self.get_csv_Data(csv_input_file,flow_output_file,
          job_output_file,
          dag_output_file,
          owner_output_file,schedule_output_file,flow_exec_output_file,job_exec_output_file,lineage_output_file,dataset_output_file)
    

          
    

if __name__ == "__main__":
  args = sys.argv[1]

  # connection
    # username = args[Constant.GP_DB_USERNAME_KEY]
  #password = args[Constant.GP_DB_PASSWORD_KEY]
  #JDBC_DRIVER = args[Constant.GP_DB_DRIVER_KEY]
  #JDBC_URL = args[Constant.GP_DB_URL_KEY]

  e = CSVExtract()
  #e.conn_db = zxJDBC.connect(JDBC_URL, username, password, JDBC_DRIVER)
  temp_dir = FileUtil.etl_temp_dir(args, "CSV")
  csv_input_file = args[Constant.CSV_INPUT]
  flow_output_file = os.path.join(temp_dir, args[Constant.CSV_FLOW])
  job_output_file = os.path.join(temp_dir, args[Constant.CSV_JOB])
  dag_output_file = os.path.join(temp_dir, args[Constant.CSV_DAG])
  owner_output_file = os.path.join(temp_dir, args[Constant.CSV_OWNER])
  schedule_output_file = os.path.join(temp_dir, args[Constant.CSV_SCHEDULE])
  flow_exec_output_file = os.path.join(temp_dir, args[Constant.CSV_FLOW_EXEC])
  job_exec_output_file = os.path.join(temp_dir, args[Constant.CSV_JOB_EXEC])
  lineage_output_file = os.path.join(temp_dir, args[Constant.CSV_LINEAGE])
  dataset_output_file = os.path.join(temp_dir, args[Constant.CSV_DATASET])
  
      
  

  #try:
   

  e.run(csv_input_file,
          flow_output_file,
          job_output_file,
          dag_output_file,
          owner_output_file,
          schedule_output_file,
          flow_exec_output_file,
          job_exec_output_file,
          lineage_output_file,
          dataset_output_file)
  #finally:
    #e.conn_db.cursor().close()
    #e.conn_db.close()
