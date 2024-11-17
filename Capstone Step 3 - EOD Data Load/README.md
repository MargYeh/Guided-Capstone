## Guided Capstone 3 - EOD Writeup
This part of the project uses locally stored parquet files previously partitioned into Trades and Quotes, and then cleans them by removing excess columns that are not used. Duplicate exchanges are also removed and replaced with only the latest exchange.
When cleaning the duplicate entries, exchanges are identified uniquely by trade_dt, symbol, event_tm, exchange, and event_seq_nb. Then from those that share the same identification, the one with the earliest arrival_tm will be taken. 

#Results after cleaning the trades:
![image](https://github.com/user-attachments/assets/9d175817-dfe5-4465-bba4-6f724df6d20c)

#Results after cleaning the quotes:
![image](https://github.com/user-attachments/assets/312e05b0-fa39-49ef-94fa-d24f2f6694cf)

The cleaned parquet files are uploaded to Azure Blob under the trade_date or quote_date of their first entry.
![image](https://github.com/user-attachments/assets/20cf5097-b1f0-45f2-9314-19092ace7b03)
![image](https://github.com/user-attachments/assets/7b34fadc-c82d-4629-8094-4c2551cc140a)
