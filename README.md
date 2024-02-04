# Teradata-DDL-Extraction
This repository contains script designed for extracting Data Definition Language (DDL) statements for various objects such as tables, views, stored procedures, user-defined functions (UDFs), and macros. These DDL statements are stored in a migration history table. The data within this migration history table can subsequently be utilized to convert Teradata DDLs into Databricks DDLs for the migration process.

<br>
<br>


### DDL Extraction - Notebook Parameters
- Source TD JDBC attributes (HostName, Database Name, Environment, Auth method)
- Migration History Table (MHT) path on Databricks Unity Catalog (catalog.database.table)
- Include list: Full path of a CSV file on Databricks. The SCV file contains CSV of objects to be included in the process (e.g. db1.*, db1.table1, db1.view1)
- Exclude list: Full path of a CSV file on Databricks. The SCV file contains CSV of objects to be excluded  (e.g. db2.*, db2.table1, db2.view1)


<br>
<br>


### DDL Extraction - Migration History Table (MHT)  
#### Data Columns :
- Database
- Object
- Object Type
- DDL Output
- Pass Number
- Process (DDL Extract, Transpile, DDL Apply)
- Time Stamp
- Result (Success/ Failure)
- Error Message
- Catalog


#### Migration History Table :
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/8b5dd54c-d617-49bf-914e-728deaf709f4">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/73092d63-1f00-4d16-8075-832690de7108">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/28c3e283-9f63-4586-bde4-a188e99181a9">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/d7027e02-deed-4834-9064-2e4abdf6b5fd">


<br>
<br>



### DDL Extraction - Different Flows  

#### Generic Flow / First Time Flow:
- For full extraction of all production objects (When MHT doesn't exist or when pass zero doesn’t exist) and (include-list is empty)

#### Failed objects flow (re-runs):
- MHT exists and contains pass zero and include-list is empty
- Max pass number in MHT gets incremented by 1 and all failed DDL Extract tasks in the previous pass will re-run

#### Selective Object Flow:
- Include-list is not empty
- Pass number will always be -1
- All objects in the include-list will go through the DDL Extraction process

<br>
<br>


### Databricks Cluster Compute Setup
##### For the DDL extraction, it is crucial to configure the necessary jars and libraries. Follow these steps to install the required components from the Libraries tab:

##### Jaydebeapi:
- Install the Jaydebeapi library to enable Python's seamless connectivity to Teradata. This library enhances your ability to establish efficient connections, providing a bridge for Python applications to interact with Teradata databases.

##### Terajdbc4.jar:
- Integrate the Terajdbc4.jar into the cluster. This JAR file serves as the Teradata JDBC driver, ensuring a smooth and high-performance connection between your Databricks cluster and Teradata databases.

##### Tdgssconfig.jar:
- Install the Tdgssconfig.jar library to support Teradata Global Security Services (GSS) configuration. This JAR file is vital for enhancing the security of your Databricks cluster when communicating with Teradata databases, ensuring that connections are established with the highest level of security.

<img width="576" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/35defaa4-af1f-4eac-90dd-cb1904f12ca2">


<br>
<br>


### Objects Inclusion and Exclusion CSV File Setup

##### Efficiently manage the inclusion and exclusion of objects by following these steps to set up CSV files in your Databricks workspace.

##### Inclusion CSV File Setup:

- Create File - Go to your Databricks workspace and create a file <br>
Example : file:/Workspace/Repos/apoorva.dhawan@databricks.com/Teradata-DDL-Extraction/inclusive_object_list.csv

- Format of the File:
The file should adhere to the following format: object,object_type

- Sample File : 
  - <img width="275" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/e6bcbcbc-fa71-4bca-8c82-76d5d8715baf">
  - Object, object_type : db_name.* ,   ( This includes all the objects of a database and thus we do not mention any object type ) <br>
  - Object, object_type : db_name.obj_name , object_type ( This includes only “obj_name” object from the db_name db which has given object type )

- Widget Configuration:
Add the file path to the inclusion_list_path widget


##### Exclusion CSV File Setup:

- Create File - Go to your Databricks workspace and create a file <br>
Example : file:/Workspace/Repos/apoorva.dhawan@databricks.com/Teradata-DDL-Extraction/exclusion_object_list.csv

- Format of the File:
The file should adhere to the following format: Object,object_type

- Sample File : 
  - <img width="400" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/182e75cd-8bf0-46ab-8d06-05859a0fc034">
  - Object, object_type : db.* ,   ( This includes all the objects of a database and thus we do not mention any object type ) <br>
  - Object, object_type : db.obj_name , object_type (  This includes only “obj_name” object from the db which has given object type )

- Widget Configuration:
Add the file path to the exclusion_list_path widget 

<br>
<br>



### Workflow 
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/5a18a371-ea5c-4010-87b5-58731662f645">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/448e8868-2c52-4072-827d-b96dafc0f356">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/ac6d8aae-1479-4de1-bd35-cd2c9d5cf3e1">




<br>
<br>

### Other Screenshots 

- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/79bc6b32-0713-4eb8-b0d1-c93f9341300a">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/ae83282b-7ee6-4c3d-8401-7c2840895c8a">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/1f91e02e-1b3e-4c10-9e94-083dbede4cbb">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/2bddfafd-dec6-4ffe-9fca-3a22bc8e9a4e">
- <img width="800" alt="image" src="https://github.com/ApoorvaD13/Teradata-DDL-Extraction/assets/146916128/7b31bf16-4127-4ce5-9c17-f9cbeef1211b">









