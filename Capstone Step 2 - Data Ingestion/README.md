# Data Ingestion Writeup
This is a project that takes Stock Exchange data in csv or json files in a semi-structured text format, and uses pySpark to parse this information into a Spark DataFrame. These DataFrames are outputted into a parquet file which partitions the information into Trades, Quotes, and Bad (corrupted) Records.  Files used for processing this data were uploaded into Azure Blob Storage using AzCopy. One of each type of file (Test_json and Test_csv) located at the root were used for the screenshots below, but the path can be changed to process additional files.

## CommonEvent
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

## To run: Load data into Azure Blob Storage and replace the following in data_injestion.py:
 ![image](https://github.com/user-attachments/assets/8e247bf1-ef39-43a0-97d9-abe54d666b0c)


The code will automatically download the necessary Hadoop-azure.jar and azure-storage.jar files, but these files are also included within the jars folder if needed.

## Sample Screenshots:
 ![image](https://github.com/user-attachments/assets/e3ffc85c-8064-42bd-bb88-439e87851ec1)

Top is a sample of the first 5 entries from test_json, bottom is a sample of test_csv

## Output
Spark dataframes are combined and outputted as parquet files in output_dir. These files are partitioned into those rated Q (Quotes), T (Trades), and B (Bad records)

