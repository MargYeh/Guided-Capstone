# Data Ingestion Writeup
This is a project that takes Stock Exchange data in csv or json files in a semi-structured text format, and uses pySpark to parse this information into a Spark DataFrame. These DataFrames can be outputted into a parquet file which partitions the information into Trades, Quotes, and Bad (corrupted) Records.  Files used for processing this data were uploaded into Azure Blob Storage using AzCopy. One of each type of file (Test_json and Test_csv) located at the root were used for the screenshots below.
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
The result is outputted as parquet files to HDFS through the following code:
![image](https://github.com/user-attachments/assets/64fc7aae-60e7-44cf-838c-59aa2a04d900)

