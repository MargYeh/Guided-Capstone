#%%
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta

#SparkSession Setup
spark = SparkSession.builder.appName('EOD Data Load') \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

storage_account = '<storage_account_name>'
storage_access_key = '<storage_access_key>'
container = '<container_name>'

url = 'wasbs://'+container+'@' + storage_account + '.blob.core.windows.net'
spark.conf.set( 
        "fs.azure.account.key."+storage_account+".blob.core.windows.net", 
        storage_access_key
        ) 


# %%
# Read in trade and quote data from Azure Blob Storage
trade_date = '2020-08-06'
df_trades = spark.read.parquet(url + '/trade/trade_dt={}'.format(trade_date))
df_quotes = spark.read.parquet(url + '/quote/quote_dt={}'.format(trade_date))
#df_quotes.show()

# %%
#Select the table for the trades = trade_date
df_trades.createOrReplaceTempView('trades')
sql_command = "SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr FROM trades where trade_dt = '"+trade_date+"'"
df2 = spark.sql(sql_command)

# %%
#Calculate the 30 minute moving average
df2.createOrReplaceTempView('tmp_trade_moving_avg')
sql_command = """
    SELECT 
           trade_dt, symbol, exchange, event_tm, event_seq_nb,trade_pr,
           AVG(trade_pr) OVER (PARTITION BY (symbol)
                ORDER BY to_timestamp(event_tm) 
                RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr    
    FROM tmp_trade_moving_avg
"""
mov_avg_df = spark.sql(sql_command)
mov_avg_df.show()

# %%
#Save the results for staging
spark.sql("DROP TABLE IF EXISTS temp_trade_moving_avg")
mov_avg_df.write.saveAsTable('temp_trade_moving_avg')

#spark.read.table('temp_trade_moving_avg').show()
#date = datetime.strptime(trade_date)
#------------------------------------------------
# %%
#Calculate the previous day's trades

date = datetime.strptime(trade_date, '%Y-%m-%d')
prev_date = date - timedelta(days=1)
prev_date_str = prev_date.strftime('%Y-%m-%d')

sql_command = "SELECT trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_pr FROM trades where trade_dt = '"+prev_date_str+"'"
df3 = spark.sql(sql_command)
#df3.show()
# %%
# Add in the moving average for the previous day
df3.createOrReplaceTempView('prev_tmp_trade_moving_avg')
sql_command = """
    SELECT 
           trade_dt, symbol, exchange, event_tm, event_seq_nb,trade_pr,
           AVG(trade_pr) OVER (PARTITION BY (symbol)
                ORDER BY to_timestamp(event_tm) 
                RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr    
    FROM prev_tmp_trade_moving_avg
"""

mov_avg_df2 = spark.sql(sql_command)
mov_avg_df2.show()
#%%
# get the last price of the previous table, group it by symbol and exchange
mov_avg_df2.createOrReplaceTempView('tmp_last_trade')
sql_command = """
    SELECT DISTINCT symbol, exchange, LAST_VALUE(mov_avg_pr)
    OVER (PARTITION BY symbol, exchange
    ORDER BY to_timestamp(event_tm)
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_pr
    FROM tmp_last_trade
"""
last_pr_df = spark.sql(sql_command)
last_pr_df.show()
#%%
spark.sql("DROP TABLE IF EXISTS temp_last_trade")
mov_avg_df.write.saveAsTable('temp_last_trade')


# %%
#Join it with the quotes table

#df_quotes.show()

df_quotes.withColumn('rec_type', F.lit('Q')).createOrReplaceTempView('quotes')
mov_avg_df.withColumn('rec_type', F.lit('T')).createOrReplaceTempView('temp_trade_moving_avg')

#%%
sql_command = """
    SELECT trade_dt, rec_type, symbol, event_tm, event_seq_nb, exchange,
        bid_pr, bid_size, ask_pr, ask_size, null as trade_pr, null as mov_avg_pr
        FROM quotes
    UNION ALL
    SELECT trade_dt, rec_type, symbol, event_tm, null as event_seq_nb, exchange,
        null as bid_pr, null as bid_size, null as ask_pr, null as ask_size, trade_pr, mov_avg_pr
        FROM temp_trade_moving_avg
"""
quote_union = spark.sql(sql_command)
quote_union.show()
quote_union.createOrReplaceTempView("quote_union")
# %%
# Populate it with the last trade_pr and mov_avg_pr
# from pyspark.sql.window import Window
# from pyspark.sql.functions import when, col, last
# window_spec = Window.partitionBy("symbol").orderBy(col("event_time").desc())
sql_command = """
SELECT *,
    LAST(trade_pr, TRUE) 
    OVER (PARTITION BY symbol, exchange 
          ORDER BY to_timestamp(event_tm) 
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_trade_pr,
    LAST(mov_avg_pr, TRUE) 
    OVER (PARTITION BY symbol, exchange 
          ORDER BY to_timestamp(event_tm) 
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_mov_avg_pr
FROM quote_union;
"""
quote_union_update = spark.sql(sql_command)
quote_union_update.show()
quote_union_update.createOrReplaceTempView('quote_union_update')

# %%
#Filter for quote records
sql_command = """
    SELECT trade_dt, symbol, event_tm, event_seq_nb, exchange,
    bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
    FROM quote_union_update
    WHERE rec_type = 'Q'
"""
quote_update = spark.sql(sql_command)
quote_update.show()
quote_update.createOrReplaceTempView('quote_update')
# %%
#Join with Table temp_last_trade to get the prior day's close price
last_pr_df.createOrReplaceTempView('temp_last_trade')

#%%
sql_command = """
    SELECT 
        trade_dt, symbol, event_tm, event_seq_nb, exchange, 
        bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr,
        bid_pr - close_pr AS bid_pr_mv, 
        ask_pr - close_pr AS ask_pr_mv
    FROM (
        SELECT /* + BROADCAST(t) */
            q.trade_dt, q.symbol, q.event_tm, q.event_seq_nb, q.exchange,
            q.bid_pr, q.bid_size, q.ask_pr, q.ask_size, q.last_trade_pr, q.last_mov_avg_pr,
            t.last_pr AS close_pr
        FROM 
            quote_update q
        LEFT JOIN 
            temp_last_trade t 
        ON 
            q.symbol = t.symbol 
    ) 
"""
quote_final = spark.sql(sql_command)
quote_final.show()

quote_final.write.mode("overwrite").parquet(url + '/quote-trade-analytical/date={}'.format(trade_date))
# %%
