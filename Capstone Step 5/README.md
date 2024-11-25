# Capstone Step 5- Pipeline Orchestration
The previous steps are connected to one another by using Azure Databricks

## Steps:
- Load the corresponding python files into Azure Databricks

![image](https://github.com/user-attachments/assets/8d407348-263c-465a-822f-66de0ac5900b)

- Create the compute under Compute

![image](https://github.com/user-attachments/assets/00072e1d-a837-416b-827b-e70c5703ecc6)
![image](https://github.com/user-attachments/assets/b0ae6440-30f8-4495-aa65-c227e7d27744)

- Create the Jobs under Workflows. When creating use Python Script for the Task type. This can also be scheduled directly from the python file or notebook. The jobs can be run manually from the Jobs UI page

![image](https://github.com/user-attachments/assets/914559af-a152-4a06-8efb-dba907b63576)
![image](https://github.com/user-attachments/assets/65f9df51-f349-4176-8dfd-80375cd82800)

- The results of past runs are shown under Job runs. If there are failed runs, the reason for failure can be examined within the logs kept by Databricks

![image](https://github.com/user-attachments/assets/63703a3c-642d-48ce-99ff-1d14ec6a72ae)

- In the case of failed jobs:

![image](https://github.com/user-attachments/assets/f45ae323-2f17-40d8-9443-ed1b4801652b)
![image](https://github.com/user-attachments/assets/c32a4d3a-b48c-4189-b7b2-af4e2d527547)


