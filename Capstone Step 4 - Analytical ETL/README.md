# Analytical ETL

## Description
This loads the trade and quote parquets stored on Azure Blob Storage in the previous step and performs some analysis on them. The process of analysises are:
- Daily trades are appended with an average of trades within the previous 30 minuites
- The last previous day's trades are collected for each symbol (closing price)
- The quotes through the day are appended with values from the last confirmed trade
- The quotes through the day are appended with values calculating the current bid/ask price difference from the previous day's close price.

## Setup
Replace the information below to connect to the Azure Blob Storage:

![image](https://github.com/user-attachments/assets/ca93a196-2693-48fb-9ab5-0fbffe25dd7c)

## Results
### Calculate the 30 min moving average:
```
    SELECT 
           trade_dt, symbol, exchange, event_tm, event_seq_nb,trade_pr,
           AVG(trade_pr) OVER (PARTITION BY (symbol)
                ORDER BY to_timestamp(event_tm) 
                RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr    
    FROM tmp_trade_moving_avg
```

![image](https://github.com/user-attachments/assets/18ed8164-c1ff-4421-b939-90fd206928bc)
This view is saved into a Hive table for staging later

### Repeat the 30 min moving average calculation for the previous day, and then collect only the last price 
```
    SELECT 
           trade_dt, symbol, exchange, event_tm, event_seq_nb,trade_pr,
           AVG(trade_pr) OVER (PARTITION BY (symbol)
                ORDER BY to_timestamp(event_tm) 
                RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr    
    FROM prev_tmp_trade_moving_avg
```
```
    SELECT DISTINCT symbol, exchange, LAST_VALUE(mov_avg_pr)
    OVER (PARTITION BY symbol, exchange
    ORDER BY to_timestamp(event_tm)
    RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_pr
    FROM tmp_last_trade
```
![image](https://github.com/user-attachments/assets/e308cc50-e817-4cb2-9d71-4e0c1e4741a5)

### Create a Common Schema for holding the quotes and to combine it with the new analysis from trades
The trade events happen at different times as the quote events, so equality join will not work. We want the quotes to save only the data from the last confirmed trade throughout the day. The following is the schema that will be used:
![image](https://github.com/user-attachments/assets/f687eeb7-5697-4412-81ec-bc1168b24179)

```
    SELECT trade_dt, rec_type, symbol, event_tm, event_seq_nb, exchange,
        bid_pr, bid_size, ask_pr, ask_size, null as trade_pr, null as mov_avg_pr
        FROM quotes
    UNION ALL
    SELECT trade_dt, rec_type, symbol, event_tm, null as event_seq_nb, exchange,
        null as bid_pr, null as bid_size, null as ask_pr, null as ask_size, trade_pr, mov_avg_pr
        FROM temp_trade_moving_avg
```
![image](https://github.com/user-attachments/assets/98351fa5-8ecd-4dac-b4a5-e383a322cef2)
![image](https://github.com/user-attachments/assets/2f4392bb-53f5-43ab-8ab2-33fa388cbf25)

### Populate the trade_pr and mov_avg_pr in quotes with the corresponding trades
```
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
```
![image](https://github.com/user-attachments/assets/ca0c1a3f-6528-4bed-bf46-a0b73b5c75a7)
![image](https://github.com/user-attachments/assets/4e886a07-52d2-44e8-93e6-413ff5a56704)

### Filtering only the quote records
```
    SELECT trade_dt, symbol, event_tm, event_seq_nb, exchange,
    bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
    FROM quote_union_update
    WHERE rec_type = 'Q'
```
![image](https://github.com/user-attachments/assets/99506157-149d-4c43-982f-ac3445a93c50)

### Calculate the change from the previous day's close prices by broadcasting the previous day's final prices
```
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
```
![image](https://github.com/user-attachments/assets/2a03f93d-2c75-4d64-b2b6-f6f44e41a8c0)


### Save this final result back to Azure Blob Storage
```
quote_final.write.mode("overwrite").parquet(url + '/quote-trade-analytical/date={}'.format(trade_date))
```











