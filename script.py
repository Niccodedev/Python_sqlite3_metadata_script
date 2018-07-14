import sqlite3
import sys
import time
import datetime
import os
import json
import subprocess
interval = sys.argv[1]
db = sys.argv[2]
def miliseconds_to_time(s):
    return datetime.datetime.fromtimestamp(float(s)).strftime('%Y-%m-%d %H:%M:%S')

def main(interval, db):
    while True:      
        conn = sqlite3.connect(db)
        res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")        
        tables = []
        indexes = []
        index_names = []
        for name in res:
            tables.append(name[0])
        res = conn.execute("SELECT sql , name FROM sqlite_master WHERE type='index';")
        for index in res:
            indexes.append(index[0])
            index_names.append(index[1])
        print("{:39s}{:40s}".format("Table Name", " Indexes "))
        for table in tables:
            table_indexes = []
            for i in range(len(index_names)):
                if indexes[i].find(table+'(') >- 1:
                    table_indexes.append(index_names[i])
            index_strings = ",  ".join(table_indexes)
            print("{:40s}{:40s}".format(table, index_strings))
        res = conn.execute("SELECT name, value FROM info;")
        entities = []
        entity_info = {}
        for record in res:
            if  record[0].endswith(".key"):
                entities.append(record[0].split('.')[0])
            entity_info[record[0]] = record[1]
        index_can_be_queried = entity_info["index_can_be_queried"]
        print("{:10s}*********       General Info of Entities        ************".format(''))
        print ('{:10s}{:5s}{:7s}{:27s}{:23s}{:8s}'.format('Name','Key',"Slices","Init_load_status","Times Fields","Changed?"))
        #print ('{:15s}{:13s}{:9s}{:37s}{:29s}{:15s}'.format('Entity name','Entity key',"Slices","Initial load status","Times Fields", "Entity changed?"))

        #  print ('{:20s}{:14s}{:10s}{:25s}{:30s}{:30s}{:30s}{:30s}'.format('Entity name','Entity key',"Slices","Initial load status","Last crawl time:", "Last record time:", "Last scan time:", "Entity changed?"))
        for entity in entities:
            entity_name = entity
            entity_key = entity_info[entity+'.key']
            slices = len(json.loads(entity_info[entity+'.slices']))
            initial_load_status = json.loads(entity_info[entity+'.initial_load_status'])
            current_slices = initial_load_status['current_slice']
            total_slices = initial_load_status['total_slices']
            ttotal_slices = initial_load_status['total_slices']
            last_crawl_time = miliseconds_to_time(entity_info[entity+'.last_crawl_time'])
            last_record_time = miliseconds_to_time(entity_info[entity+'.last_record_time'])
            last_scan_time = miliseconds_to_time(entity_info[entity+'.last_scan_time'])
            entity_changed = entity_info[entity+'.entity_changed']

            print ('{:10s}{:5s}{:7s}{:15s}{:3s}{:20s}{:8s}'.format(entity_name,entity_key, str(slices),"current_slice: ", str(current_slices),"last_crawl :"+last_crawl_time,"   "+str(entity_changed)))
            print('{:22s}{:15s}{:3s}{:20s}'.format('',"Total slices: ",str(total_slices),"Last record:"+last_record_time))
            print('{:40s}{:20s}'.format('',"Last scan  :"+last_scan_time))
           # print('{:62s}{:13s}{:32s}'.format('',"Last scan  :", last_scan_time))
            
        print("{:10s}*********       Tables of Entities       ************".format(''))
        print('{:10s}{:8s}{:20s}{:10s}{:32s}'.format("Name", "Rows" ,"Col_Name", "Col_Type", "Other Column Info" ))
        for entity in entities:
            entity_name = entity
            entity_table_name = "ap_" + entity_name
            sql = "select count(*) from " + entity_table_name + ';'
            res = conn.execute(sql) 
            number_of_records = None;
            for record in res:
                number_of_records = record[0]          
            res = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' and  name='" + entity_table_name + "';")
           
            for record in res:
                fields = record[0][record[0].find('(')+1:-1].split(',')           
            field = fields[0].strip()  
            column_name = field.split(" ")[0]           
            column_type = field.split(" ")[1]
            other_info = " ".join(field.split(" ")[2:])
            print('{:10s}{:8s}{:20s}{:10s}{:32s}'.format(entity_name, str(number_of_records),column_name, column_type, other_info ))
            for i in range(1, len(fields)):
                field = fields[i].strip()  
                column_name = field.split(" ")[0].split("_")[-1]         
                column_type = field.split(" ")[1]
                other_info = " ".join(field.split(" ")[2:])
                print('{:18s}{:20s}{:10s}{:32s}'.format('', column_name, column_type, other_info ))           



        print("Index can be queried?:   ", index_can_be_queried)
        time.sleep(int(interval))
        os.system("printf '\033c'")
        
       




#print '{:10s} {:3d}  {:7.2f}'.format('xxx', 123, 98)
main(interval, db)
