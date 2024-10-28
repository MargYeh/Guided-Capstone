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
trade_common = spark.read.parquet('output_dir/partition=T')
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'trade_pr')
quote_common = spark.read.parquet('output_dir/partition=Q')
quote = quote_common.select('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'bid_pr')

# %%
def applyLatest(trade):
#     #Uses trade_dt, symbol, exchange, event_tm, event_seq_nb as the unique identifier, and returns the one with the most recent arrival_tm
    latestgroup = trade.groupBy('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb')\
            .agg({'arrival_tm': 'max'})
    corrected = latestgroup.select('trade_dt').collect()[0][0]
    return corrected
trade_corrected = applyLatest(trade)
quote_corrected = applyLatest(quote)

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


trade.write.mode("overwrite").parquet(url + '/trade/trade_dt={}'.format(trade_corrected))
quote.write.mode("overwrite").parquet(url + '/trade/trade_dt={}'.format(quote_corrected))

# %%
