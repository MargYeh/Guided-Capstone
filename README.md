# Guided_Capstone
This is the Guided Capstone project for SpringBoard's Data Engineering bootcamp. It uses Pyspark to read csv or json files stored on Azure Blob Storage and converts them into Spark Dataframes to be cleaned and analyzed. The data is in the form of daily submission of stock exchanges from Spring Capital data sources

## Step 1 - Flowchart of Process
![image](https://github.com/user-attachments/assets/38f1683e-8cba-47d8-bef9-983073d7e24b)

## Step 2 - Data Ingestion
The project reads in data in a semi-structured text format (CSV and JSON files) that are stored in Azure Blob Storage. These files are originally sourced from Spring Capital daily stock exchange submissions. The data is organized into objects that fit the schema below, and then made into Spark DataFrames and outputted in parquet files, organized by whether they are trades (Partition=T) or quotes (Partition=Q). All bad records are removed and partitioned into Partition B.
### CommonEvent
All events are loaded as CommonEvent objects, which uses the following schema:
| Column  	| Type 		|
| ------------- 	| ------------- 	|
| trade_dt  	| DateType  	|
| rec_type 	| StringType 	|
| symbol 	| StringType 	|
| exchange 	| StringType 	|
| event_tm 	| DateType 	|
| event_seq_nb | IntegerType 	|
| arrival_tm 	| DateType	|
| trade_pr 	| DecimalType 	|
| trade_size 	| IntegerType 	|
| bid_pr 	| DecimalType 	|
| bid_size 	| IntegerType 	|
| ask_pr 	| DecimalType 	|
| ask_size 	| IntegerType 	|
| partition 	| StringType 	|
| errormsg 	| StringType 	|


## Step 3 - EOD Cleaning
This part of the project uses locally stored parquet files previously partitioned into Trades and Quotes, and then cleans them by removing excess columns that are not used. Duplicate exchanges are also removed and replaced with only the latest exchange.
When cleaning the duplicate entries, exchanges are identified uniquely by trade_dt, symbol, event_tm, exchange, and event_seq_nb. Then from those that share the same identification, the one with the earliest arrival_tm will be taken. 

The cleaned parquet files are uploaded to Azure Blob under the trade_date or quote_date of their first entry.

![image](https://github.com/user-attachments/assets/20cf5097-b1f0-45f2-9314-19092ace7b03)
![image](https://github.com/user-attachments/assets/7b34fadc-c82d-4629-8094-4c2551cc140a)


