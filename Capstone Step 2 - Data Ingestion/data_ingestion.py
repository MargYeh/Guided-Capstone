#%%
# import findspark
# import os
# import time
# findspark.init('C:/spark', edit_rc=True)
# os.environ['SPARK_HOME'] = 'C:/spark'
# os.environ['HADOOP_HOME'] = 'C:/hadoop'

from pyspark.sql import SparkSession 
from datetime import datetime
import json


class Common_Event:
    def __init__(self, trade_dt=datetime.now(), rec_type="", symbol="", exchange="", event_tm=datetime.now(), event_seq_nb=0, arrival_tm=datetime.now(), trade_pr=0.0, trade_size=0, bid_pr=0.0, bid_size=0, ask_pr=0.0, ask_size=0, partition="",errormsg=""):
        self.trade_dt = trade_dt #Trade date
        self.rec_type = rec_type #Event type
        self.symbol = symbol #Symbol 
        self.exchange = exchange #Exchange
        self.event_tm = event_tm #Event time
        self.event_seq_nb = event_seq_nb #Event Sequence
        self.arrival_tm = arrival_tm #File_tm
        self.trade_pr = trade_pr #Trade Price
        self.trade_size = trade_size #Trade Size
        self.bid_pr = bid_pr #Bid Price
        self.bid_size = bid_size #Bid Size
        self.ask_pr = ask_pr #Ask Price
        self.ask_size = ask_size # Ask Size
        self.partition = partition #Partition
        self.errormsg = errormsg #Line in case of bad line

    def show(self):
        print('-------------')
        print(f'Trade date: {self.trade_dt}')
        print(f'Event type: {self.rec_type}')
        print(f'Symbol {self.symbol}')
        print(f'Exchange {self.exchange}')
        print(f'Event time: {self.event_tm}')
        print(f'Event sequence: {self.event_seq_nb}')
        print(f'File_tm: {self.arrival_tm}')
        print(f'Trade Price: {self.trade_pr}')
        print(f'Trade Size: {self.trade_size}')
        print(f'Bid Price: {self.bid_pr}')
        print(f'Bid Size: {self.bid_size}')
        print(f'Ask Price: {self.ask_pr}')
        print(f'Ask Size: {self.ask_size}')
        print(f'Partition: {self.partition}')
        print(f'Line details (In case of error):{self.errormsg}')
        print('-------------')

def parse_csv(line:str):
    record_type_pos = 2
    record = line.split(",")
    try:
        #logic to parse records
        if record[record_type_pos] == "T":
            event =  Common_Event(record[0], #Trade date
                                  "T", #Event Type
                                  record[3], #Symbol
                                  record[6], #Exchange
                                  record[4], #Event time
                                  record[5], #Event Sequence
                                  record[1], #File_tm
                                  record[7], #Trade Price
                                  record[8], #Trade Size
                                  partition='T'
                                  )
            return event
        
        elif record[record_type_pos] == "Q":
            event =  Common_Event(record[0], #Trade date
                                  "Q", #Event Type
                                  record[3], #Symbol
                                  record[6], #Exchange
                                  record[4], #Event time
                                  record[5], #Event Sequence
                                  record[1], #File_tm
                                    #Trade Price (None)
                                  bid_pr=record[7], #Bid Price
                                  bid_size=record[8], #Bid Size
                                  ask_pr=record[9],
                                  ask_size=record[10],
                                  partition='Q'
                                  )
            return event
    except:
        event = Common_Event(partition='B',errormsg=line)
        return event

def parse_json(line:str):
    record = json.loads(line)
    
    record_type = record['event_type']
    try:
        #logic to parse records
        if record_type == "T":
            #Get the applicable fields from json]
            event =  Common_Event(record['trade_dt'], #Trade date
                                  "T", #Event Type
                                  record['symbol'], #Symbol
                                  record['exchange'], #Exchange
                                  record['event_tm'], #Event time
                                  record['event_seq_nb'], #Event Sequence
                                  record['file_tm'], #File_tm
                                  record['price'], #Trade Price
                                  record['size'], #trade_size
                                  partition='T'
                                  )
            return event
        elif record_type == "Q":
            event =  Common_Event(record['trade_dt'], #Trade date
                                  "Q", #Event Type
                                  record['symbol'], #Symbol
                                  record['exchange'], #Exchange
                                  record['event_tm'], #Event time
                                  record['event_seq_nb'], #Event Sequence
                                  record['file_tm'], #File_tm
                                    #Trade Price (None)
                                  bid_pr=record['bid_pr'], #Bid Price
                                  bid_size=record['bid_size'], #Bid Size
                                  ask_pr=record['ask_pr'], #ask price
                                  ask_size=record['ask_size'],
                                  partition='Q'
                                  )
            return event
        
    except:
        #save record to dummy event in bad partition
        #fill fields as none or empty string
        event = Common_Event(partition='B',errormsg=line)
        return event

def session_setup(storage_account, storage_access_key):

#---- Build the spark connection ------
    spark = SparkSession.builder.master('local').appName('app') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6")\
        .getOrCreate()
        #  .config("spark.jars.packages", "/jars/azure-storage-8.6.5.jar,/jars/hadoop-azure-3.3.0.jar")\   
        #  .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6")\

    #---- Change these settings for account key and storage names
    spark.conf.set( 
        "fs.azure.account.key."+storage_account+".blob.core.windows.net", 
        storage_access_key
        ) 
    return spark

def get_json(spark, storage, container, location):
    raw = spark.sparkContext.textFile("wasbs://"+container+"@"+storage+".blob.core.windows.net/"+location) 
    parsed = raw.map(lambda line: parse_json(line)) 
    data = spark.createDataFrame(parsed)
    return data

def get_csv(spark, storage, container, location):
    raw = spark.sparkContext.textFile("wasbs://"+container+"@"+storage+".blob.core.windows.net/"+location)
    parsed = raw.map(lambda line: parse_csv(line)) 
    data = spark.createDataFrame(parsed)
    return data
#%%

# storage_account = '<storage_account_name>'
# storage_access_key = '<storage_access_key>'
# container = '<container_name>'

spark = session_setup(storage_account, storage_access_key)
#%%
data = get_json(spark, storage_account, container, "Test_json.txt")
data.limit(5).show() #Sample of 5 from json for testing purposes
data2 = get_csv(spark, storage_account, container, "Test_csv.txt")
data2.limit(5).show() #Sample of 5 from csv file for testing purposes
#%%
combined_data = data.union(data2)
combined_data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

#Testing if spark works------------ Remove later
#%%
# def test_spark_session():
#     spark = SparkSession.builder.getOrCreate()
#     df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
#     #df.write.option("header",True).partitionBy("id").mode("overwrite").parquet("output.parquet")
#     result = df.collect()
#     print(result)

# test_spark_session()

# %%
