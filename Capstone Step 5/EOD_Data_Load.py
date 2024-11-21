#%%
import findspark
import os
findspark.init('C:/spark', edit_rc=True)
os.environ['SPARK_HOME'] = 'C:/spark'
os.environ['HADOOP_HOME'] = 'C:/hadoop'

from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName('EOD Data Load').config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6").getOrCreate()

#%%
#Read trade partition dataset and select the necessary columns

trade_common = spark.read.parquet('../Capstone Step 2 - Data Ingestion/output_dir/partition=T')
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'bid_pr', 'bid_size', 'trade_pr', 'trade_size') #for trades, bid_pr and bid_size also contain the trade price and size
quote_common = spark.read.parquet('../Capstone Step 2 - Data Ingestion/output_dir/partition=Q')
quote = quote_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'bid_pr', 'bid_size', 'ask_pr', 'ask_size')

# %%
from pyspark.sql import functions
def trade_applyLatest(trade):
#Uses trade_dt, symbol, exchange, event_tm, event_seq_nb as the unique identifier to group
#Returns the one with the most recent arrival_tm per group
    latestgroup = trade.orderBy('arrival_tm') \
            .groupBy('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb')\
            .agg(functions.collect_set('arrival_tm'), functions.collect_set('bid_pr'), functions.collect_set('bid_size'))
    
    #Take the top of each column per set and drop the extra columns
    cleanedgroup = latestgroup.withColumn('arrival_tm', functions.slice(latestgroup['collect_set(arrival_tm)'],1,1)[0]) \
            .withColumn('trade_pr', functions.slice(latestgroup['collect_set(bid_pr)'],1,1)[0]) \
            .withColumn('trade_size', functions.slice(latestgroup['collect_set(bid_size)'],1,1)[0]) \
            .drop(functions.col('collect_set(arrival_tm)')) \
            .drop(functions.col('collect_set(bid_pr)')) \
            .drop(functions.col('collect_set(bid_size)'))
    #cleanedgroup.show()  
    #.agg({'arrival_tm': 'max'})
    #corrected = latestgroup.select('trade_dt').collect()[0][0]
    return cleanedgroup
trade_corrected = trade_applyLatest(trade)

#%%
def quote_applyLatest(quote):
#Uses trade_dt, symbol, exchange, event_tm, event_seq_nb as the unique identifier to group
#Returns the one with the most recent arrival_tm per group
    latestgroup = quote.orderBy('arrival_tm') \
            .groupBy('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb')\
            .agg(functions.collect_set('arrival_tm'), \
                 functions.collect_set('bid_pr'), \
                 functions.collect_set('bid_size'), \
                 functions.collect_set('ask_pr'), \
                 functions.collect_set('ask_size')
                )

    #Take the top of each column per set and drop the extra columns
    cleanedgroup = latestgroup.withColumn('arrival_tm', functions.slice(latestgroup['collect_set(arrival_tm)'],1,1)[0]) \
            .withColumn('bid_pr', functions.slice(latestgroup['collect_set(bid_pr)'],1,1)[0]) \
            .withColumn('bid_size', functions.slice(latestgroup['collect_set(bid_size)'],1,1)[0]) \
            .withColumn('ask_pr', functions.slice(latestgroup['collect_set(ask_pr)'],1,1)[0]) \
            .withColumn('ask_size', functions.slice(latestgroup['collect_set(ask_size)'],1,1)[0]) \
            .drop(functions.col('collect_set(arrival_tm)')) \
            .drop(functions.col('collect_set(bid_pr)')) \
            .drop(functions.col('collect_set(bid_size)')) \
            .drop(functions.col('collect_set(ask_pr)')) \
            .drop(functions.col('collect_set(ask_size)'))
    #cleanedgroup.show()

    return cleanedgroup
    

quote_corrected = quote_applyLatest(quote)

# %%
#Write the trade dataset back to parquet on Azure Blob Storage

storage_account = '<storage_account_name>'
storage_access_key = '<storage_access_key>'
container = '<container_name>'

url = 'wasbs://'+container+'@' + storage_account + '.blob.core.windows.net'
spark.conf.set( 
        "fs.azure.account.key."+storage_account+".blob.core.windows.net", 
        storage_access_key
        ) 

trade_date = trade_corrected.select('trade_dt').collect()[0][0]
trade_corrected.write.mode("overwrite").parquet(url + '/trade/trade_dt={}'.format(trade_date))

quote_date = quote_corrected.select('trade_dt').collect()[0][0]
quote_corrected.write.mode("overwrite").parquet(url + '/quote/quote_dt={}'.format(quote_date))

# %%
