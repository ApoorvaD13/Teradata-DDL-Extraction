# Databricks notebook source
##RUN ONCE ONLY FOR DEBUGGING WITHIN NOTEBOOK

dbutils.widgets.text("source_database", "NS_PROD_DATA")
dbutils.widgets.text("source_auth", "LDAP")
dbutils.widgets.text("source_host", "NSTERAPR.ATLDC.NSCORP.COM")
dbutils.widgets.text("source_env", "prod")
dbutils.widgets.text("inclusion_list_path", "", "")
dbutils.widgets.text("exclusion_list_path", "", "")
dbutils.widgets.text("migration_table_db_name", "",)

# COMMAND ----------

# AWS SECRET MANAGER CODE : 

# Note : 
# 1.Install boto3 in the cluster 
# 2.Uncomment code in cmd-2
# 3.Replace code in cmd-3 with this :
#     # '''
#     #         db = dbutils.widgets.get("source_database")
#     #         auth = dbutils.widgets.get("source_auth")
#     #         host = dbutils.widgets.get("source_host")
#     #         env = dbutils.widgets.get("source_env")
#     #         # scope = f"nscorp_login_{env}"                        #--------------------------------------> Remove this - not needed with aws secrets
#     #         user = credentials['teradata_username']                #--------------------------------------> updated with AWS secrets
#     #         password = credentials['teradata_password']            #--------------------------------------> updated with AWS secrets
#     #         inclusion_csv_file_path = dbutils.widgets.get("inclusion_list_path")
#     #         exclusion_csv_file_path = dbutils.widgets.get("exclusion_list_path")

#     #         mht_db_table = dbutils.widgets.get("migration_table_db_name")
#     #         mht_db = mht_db_table.split('.')[0]
#     #         migration_table_name = mht_db_table.split('.')[1]
#     # '''


################################## AWS Secret Manager 
# import json
# import boto3

# # Set AWS credentials
# aws_access_key_id = "access-key-id"
# aws_secret_access_key = "secret-access-key"

# # Set AWS region
# aws_region = "region"

# # Create an AWS Secrets Manager client
# secrets_manager_client = spark._jsc._gateway.jvm.boto3.client(
#     'secretsmanager',
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key,
#     region_name=aws_region
# )

# # Specify the secret name
# secret_name = "secret-name"

# # Retrieve the secret value
# response = secrets_manager_client.getSecretValue(secretId=secret_name)
# secret_string = response['SecretString']

# # Parse the JSON string to get the username and password
# credentials = json.loads(secret_string)

# COMMAND ----------

db = dbutils.widgets.get("source_database")
auth = dbutils.widgets.get("source_auth")
host = dbutils.widgets.get("source_host")
env = dbutils.widgets.get("source_env")
scope = f"nscorp_login_{env}"
user = dbutils.secrets.get(scope, key="teradata_username_key")
password = dbutils.secrets.get(scope, key="teradata_password_key")
inclusion_csv_file_path = dbutils.widgets.get("inclusion_list_path")
exclusion_csv_file_path = dbutils.widgets.get("exclusion_list_path")

mht_db_table = dbutils.widgets.get("migration_table_db_name")
mht_db = mht_db_table.split('.')[0]
migration_table_name = mht_db_table.split('.')[1]

# COMMAND ----------

# Import necessary PySpark and other libraries
from pyspark.sql.functions import col, lit, trim, udf, current_timestamp, when, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from functools import reduce
import jaydebeapi

# Define a class for Teradata object extraction
class TeradataObjectExtraction:
    def __init__(self, source_database_widget_name, source_auth_widget_name, source_host_widget_name, source_env_widget_name, user, password, mht_db):
        # Initialize class attributes with provided parameters
        self.db = source_database_widget_name
        self.auth = source_auth_widget_name
        self.host = source_host_widget_name
        self.env = source_env_widget_name

        self.user = user
        self.password = password

        # Construct Teradata connection URL
        self.url = "jdbc:teradata://{teradata_host}/DATABASE={database},LOGMECH={auth},USER={username},PASSWORD={password}".\
            format(teradata_host=self.host, database=self.db, auth=self.auth, username=self.user, password=self.password)
        
        self.driver = "com.teradata.jdbc.TeraDriver"
        self.object_store_db = mht_db 

    # Method to create a database if it doesn't exist
    def create_database(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.object_store_db}")

    # Method to get a sorted list of Teradata databases
    def get_sorted_db_list(self):
        query = "SELECT DatabaseName FROM DBC.DATABASESV WHERE DBKind = 'D'"
        jdbc_options = {
            "url": self.url,
            "dbtable": f"( {query} ) tmp",
            "driver": self.driver
        }
        dfr = spark.read.format("jdbc").options(**jdbc_options)
        dataframe = dfr.load()
        db_list = dataframe.select('DatabaseName').rdd.flatMap(lambda x: x).collect()
        sorted_db_list = sorted(db_list)
        return sorted_db_list

    # Method to get a dictionary of objects for each database
    def get_object_list(self, object):
        object_db_ddl_dict = {}
        for db in self.get_sorted_db_list():
            query = "select databasename,TableName from DBC.TablesV where databasename = '{0}' and tablekind = '{1}'".format(db, object)
            jdbc_options = {
                "url": self.url,
                "dbtable": f"( {query} ) tmp",
                "driver": self.driver,
            }
            object_db_ddl = [(i[0], i[1]) for i in spark.read.format("jdbc").options(**jdbc_options).load().collect()]
            object_db_ddl_dict[db] = object_db_ddl
        return object_db_ddl_dict

    # Method to get a dictionary of objects for a specific database
    def get_object_list_for_database(self, target_database, object):
        query = "select databasename,TableName from DBC.TablesV where databasename = '{0}' and tablekind = '{1}'".format(target_database, object)
        jdbc_options = {
            "url": self.url,
            "dbtable": f"( {query} ) tmp",
            "driver": self.driver,
        }
        object_db_ddl = [(i[0], i[1]) for i in spark.read.format("jdbc").options(**jdbc_options).load().collect()]
        object_db_ddl_dict = {target_database: object_db_ddl}
        return object_db_ddl_dict

    # Method to get all objects for a specific database
    def get_all_objects_for_database(self, target_database):
        object_types = ["T", "V", "F", "P", "M"]
        all_objects_dict = {}

        for object_type in object_types:
            object_db_ddl_dict = self.get_object_list_for_database(target_database, object_type)
            objects_list = []

            for db, objects in object_db_ddl_dict.items():
                objects_list.extend(objects)

            all_objects_dict[object_type] = objects_list

        return all_objects_dict


# COMMAND ----------

# Define a class for Teradata DDL (Data Definition Language) extraction
class TeradataDDLExtractor:
    def __init__(self, user, password, db, auth, host):
        # Initialize class attributes with provided parameters
        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.auth = auth
        # Construct Teradata connection URL and set necessary driver and JAR files
        self.url = f"jdbc:teradata://{self.host}/DATABASE={self.db},LOGMECH={self.auth}"
        self.driver = "com.teradata.jdbc.TeraDriver"
        self.jars = ["/dbfs/FileStore/terajdbc4.jar", "/dbfs/FileStore/tdgssconfig.jar"]

    # Method to extract DDL (Data Definition Language) for a specific object in a Teradata database
    def extract_ddl(self, db, object, table_kind):
        # Establish a connection to Teradata using jaydebeapi
        conn = jaydebeapi.connect(self.driver, self.url, [self.user, self.password], self.jars)
        cursor = conn.cursor()
        try:
            # Execute a SHOW query to retrieve DDL information
            query = 'SHOW {0} {1}."{2}"'.format(table_kind, db, object)
            cursor.execute(query)
            row = cursor.fetchall()
            # Close the resources after fetching the result
            cursor.close()
            conn.close()
            # Extracting the DDL output from the result and handling line breaks
            ddl_output = row[0][0] if row else ""
            ddl_output = ddl_output.replace('\r', '\n')
            return ddl_output, None
        except Exception as e:
            # Return an empty string and the error message in case of an exception
            return "", str(e)

    # Method to create a DataFrame with the result of DDL extraction
    def create_result_ddl_df(self, input_df):
        # Extract individual columns from the struct
        result_df = input_df.withColumn("ddl_output", input_df["ddl_output_struct.ddl_output"])
        result_df = result_df.withColumn("error_message", input_df["ddl_output_struct.error_message"])
        # Drop the intermediate struct column
        result_df = result_df.drop("ddl_output_struct")
        return result_df


# COMMAND ----------

# Define a helper class containing various methods for object extraction and processing
class Helper:
    def get_wilcard_db_objects(self, teradata_extractor, df, schema):
        # Iterate over the DataFrame and find databases where object="*"
        dbs_with_asterisk = df.filter(col("object") == "*").select("db").distinct()

        # Create empty DataFrames for each object type
        df_table = spark.createDataFrame([], schema)
        df_view = spark.createDataFrame([], schema)
        df_macro = spark.createDataFrame([], schema)
        df_procedure = spark.createDataFrame([], schema)
        df_udf = spark.createDataFrame([], schema)

        # Iterate over the DataFrame and extract objects for each database where object="*"
        for db_row in dbs_with_asterisk.rdd.collect():
            target_database = db_row["db"]

            # Extract all objects for each type within the target database
            all_objects_for_database = teradata_extractor.get_all_objects_for_database(target_database)

            # Access the result for tables, views, macros, procedures, and UDFs within the target database
            table_list = all_objects_for_database["T"]
            view_list = all_objects_for_database["V"]
            macro_list = all_objects_for_database["M"]
            procedure_list = all_objects_for_database["P"]
            udf_list = all_objects_for_database["F"]

            # Combine the lists into DataFrames
            if table_list:
                tables = spark.createDataFrame(table_list, schema)
                df_table = df_table.unionByName(tables)
            if view_list:
                views = spark.createDataFrame(view_list, schema)
                df_view = df_view.unionByName(views)
            if macro_list:
                macros = spark.createDataFrame(macro_list, schema)
                df_macro = df_macro.union(macros)
            if procedure_list:
                procedures = spark.createDataFrame(procedure_list, schema)
                df_procedure = df_procedure.union(procedures)
            if udf_list:
                udfs = spark.createDataFrame(udf_list, schema)
                df_udf = df_udf.union(udfs)

        return df_table, df_view, df_macro, df_procedure, df_udf

    def get_objects(self, df, obj_type):
        # Filter rows where object_type matches
        df_obj_append = df.filter(col("object_type") == obj_type).select("db", "object")
        return df_obj_append

    def check_for_exclusion(self, exclusion_csv_file_path):
        # Check if an exclusion CSV file path is provided
        if exclusion_csv_file_path:
            exclude_df = spark.read.csv(exclusion_csv_file_path, header=True, inferSchema=True)
            return True, exclude_df
        return False, []

    def get_excluded_objects(self, teradata_extractor, exclusion_csv_file_path, schema):
        # Check for exclusion, read CSV file, and create DataFrames for each object type
        exclusion_flag, exclude_df = self.check_for_exclusion(exclusion_csv_file_path)

        df_table = spark.createDataFrame([], schema)
        df_view = spark.createDataFrame([], schema)
        df_macro = spark.createDataFrame([], schema)
        df_procedure = spark.createDataFrame([], schema)
        df_udf = spark.createDataFrame([], schema)

        if exclusion_flag:
            # Split the "object" column based on "."
            split_col = split(exclude_df["object"], "\.")
            df = exclude_df.withColumn("db", split_col.getItem(0)) \
                .withColumn("object", split_col.getItem(1))

            # Fetch data for wildcard databases
            df_table, df_view, df_macro, df_procedure, df_udf = self.get_wilcard_db_objects(teradata_extractor, df, schema)
            # Add given objects to specific object DataFrames
            df_table = df_table.unionByName(self.get_objects(df, "table"))
            df_view = df_view.unionByName(self.get_objects(df, "view"))
            df_macro = df_macro.unionByName(self.get_objects(df, "macro"))
            df_procedure = df_procedure.unionByName(self.get_objects(df, "procedure"))
            df_udf = df_udf.unionByName(self.get_objects(df, "udf"))

        return df_table, df_view, df_macro, df_procedure, df_udf

    def extract_and_create_dataframe(self, teradata_extractor, object_type, sorted_db_list, schema):
        # Extract object list for each database and create a DataFrame
        object_db_ddl_dict = teradata_extractor.get_object_list(object_type)
        data = []
        for db in sorted_db_list:
            data += object_db_ddl_dict[db]
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        return df

    def generate_ddl(self, input_df, teradata_ddl_extractor, object_type):
        # Generate DDL and create a DataFrame with the result
        ddl_output_column = "ddl_output_struct"
        input_df = input_df.withColumn(ddl_output_column, extract_ddl_udf(input_df["db"], input_df["object"], lit(object_type)))
        result_df = teradata_ddl_extractor.create_result_ddl_df(input_df)
        # Adding the object_type column
        result_df = result_df.withColumn("object_type", lit(object_type))
        return result_df

    def create_or_append_table_migration_history_table(self, df, database_name, table_name):
        # Check if the table exists
        ##Update to use correct migration history catalog
        spark.sql("USE CATALOG hive_metastore;")
        ##move this out so that we only check if table is created once
        table_exists = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{table_name}'").count() > 0
        if not table_exists:
            # Create the table if it doesn't exist
            df.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")
            print(f"Table {database_name}.{table_name} successfully created")
        else:
            # Append data to the existing table
            df.write.format("delta").mode("append").saveAsTable(f"{database_name}.{table_name}")
            print(f"Table {database_name}.{table_name} successfully appended")

    def selective_object_flow(self, input_df):
        # Split the "object" column based on "."
        split_col = split(input_df["object"], "\.")
        df = input_df.withColumn("db", split_col.getItem(0)) \
            .withColumn("object", split_col.getItem(1))

        df = df.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))\
            .withColumn("object_type", trim(col("object_type")))

        # Fetch data for wildcard databases
        df_table, df_view, df_macro, df_procedure, df_udf = self.get_wilcard_db_objects(teradata_extractor, df, schema)

        # Add given objects to specific object DataFrames
        df_table = df_table.unionByName(self.get_objects(df, "table"))
        df_view = df_view.unionByName(self.get_objects(df, "view"))
        df_macro = df_macro.unionByName(self.get_objects(df, "macro"))
        df_procedure = df_procedure.unionByName(self.get_objects(df, "procedure"))
        df_udf = df_udf.unionByName(self.get_objects(df, "udf"))

        return df_table, df_view, df_macro, df_procedure, df_udf

    def generic_flow(self):
        # Extract all objects for each type
        # Get sorted DB list for associated teradata
        sorted_db_list = teradata_extractor.get_sorted_db_list()
        df_table = self.extract_and_create_dataframe(teradata_extractor, "T", sorted_db_list, schema)
        df_view = self.extract_and_create_dataframe(teradata_extractor, "V", sorted_db_list, schema)
        df_procedure = self.extract_and_create_dataframe(teradata_extractor, "P", sorted_db_list, schema)
        df_macro = self.extract_and_create_dataframe(teradata_extractor, "M", sorted_db_list, schema)
        df_udf = self.extract_and_create_dataframe(teradata_extractor, "F", sorted_db_list, schema)

        return df_table, df_view, df_macro, df_procedure, df_udf


# COMMAND ----------

# Define a class for Teradata migration pipeline
class TeradataMigrationPipeline:
    def __init__(self, inclusion_csv_file_path, helper_instance, teradata_ddl_extractor):
        # Initialize class attributes with provided parameters
        self.inclusion_csv_file_path = inclusion_csv_file_path
        self.helper_instance = helper_instance
        self.teradata_ddl_extractor = teradata_ddl_extractor

    # Method to decide the flow based on the presence of inclusion CSV file
    def decide_flow(self):
        # Initialize DataFrames to empty strings
        df_table = df_view = df_macro = df_procedure = df_udf = ""
        pass_number = ""
        # Check if migration history table exists
        mht_table_exists = spark.sql(f"SHOW TABLES IN {teradata_extractor.object_store_db} LIKE '{migration_table_name}'").count()
        if not mht_table_exists and not self.inclusion_csv_file_path:
            # Generic Flow
            pass_number = 0
            df_table, df_view, df_macro, df_procedure, df_udf = self.helper_instance.generic_flow()
        elif mht_table_exists and not self.inclusion_csv_file_path:
            # Failure Cases Re-Run Flow 
            query = "select max(pass_number) from {db}.{table}".format(db=teradata_extractor.object_store_db, table=migration_table_name)
            pass_number = spark.sql(query).collect()[0][0]
            pass_number = pass_number + 1
        elif (mht_table_exists or not mht_table_exists) and self.inclusion_csv_file_path:
            # Selective Object Extraction Flow
            pass_number = -1
            input_df = spark.read.csv(self.inclusion_csv_file_path, header=True, inferSchema=True)
            df_table, df_view, df_macro, df_procedure, df_udf = self.helper_instance.selective_object_flow(input_df)

        return df_table, df_view, df_macro, df_procedure, df_udf, pass_number

    # Method to exclude objects based on an exclusion CSV file
    def exclusion(self, df_table, df_view, df_macro, df_procedure, df_udf):
        # Get excluded objects DataFrames based on the exclusion CSV file
        df_table_excluded, df_view_excluded, df_macro_excluded, df_procedure_excluded, df_udf_excluded = \
            self.helper_instance.get_excluded_objects(teradata_extractor, exclusion_csv_file_path, schema)

        df_table_excluded = df_table_excluded.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        df_view_excluded = df_view_excluded.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        df_macro_excluded = df_macro_excluded.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        df_procedure_excluded = df_procedure_excluded.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        df_udf_excluded = df_udf_excluded.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))

        # Perform left anti join to exclude objects
        df_table = df_table.join(df_table_excluded, (df_table.db == df_table_excluded.db) & (df_table.object == df_table_excluded.object), "left_anti")
        df_view = df_view.join(df_view_excluded, (df_view.db == df_view_excluded.db) & (df_view.object == df_view_excluded.object), "left_anti")
        df_procedure = df_procedure.join(df_procedure_excluded, (df_procedure.db == df_procedure_excluded.db) & (df_procedure.object == df_procedure_excluded.object), "left_anti")
        df_macro = df_macro.join(df_macro_excluded, (df_macro.db == df_macro_excluded.db) & (df_macro.object == df_macro_excluded.object), "left_anti")
        df_udf = df_udf.join(df_udf_excluded, (df_udf.db == df_udf_excluded.db) & (df_udf.object == df_udf_excluded.object), "left_anti")

        return df_table, df_view, df_macro, df_procedure, df_udf

    # Method to fetch DDL (Data Definition Language) for each object type
    def fetch_ddl(self, df_table, df_view, df_macro, df_procedure, df_udf):
        # Generate DDL for each object type and create result DataFrames
        result_df_table = self.helper_instance.generate_ddl(df_table, self.teradata_ddl_extractor, "table")
        result_df_view = self.helper_instance.generate_ddl(df_view, self.teradata_ddl_extractor, "view")
        result_df_procedure = self.helper_instance.generate_ddl(df_procedure, self.teradata_ddl_extractor, "procedure")
        result_df_macro = self.helper_instance.generate_ddl(df_macro, self.teradata_ddl_extractor, "macro")
        result_df_udf = self.helper_instance.generate_ddl(df_udf, self.teradata_ddl_extractor, "udf")

        # Trim whitespace from 'db' and 'object' columns
        result_df_table = result_df_table.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        result_df_view = result_df_view.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        result_df_procedure = result_df_procedure.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        result_df_macro = result_df_macro.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))
        result_df_udf = result_df_udf.withColumn("db", trim(col("db"))).withColumn("object", trim(col("object")))

        return result_df_table, result_df_view, result_df_macro, result_df_procedure, result_df_udf
    

    ##Included process and catalog as parameters to the function
    def featurise_migration_objects_df(self, df, pass_number_value, process, catalog):
        # Add columns to the DataFrame for migration history
        df = df.withColumn("pass_number", lit(pass_number_value)) \
            .withColumn("process", lit(process)) \
            .withColumn("catalog", lit(catalog)) \
            .withColumn("timestamp", current_timestamp()) \
            .withColumn("result", when(df["error_message"].isNull(), "Success").otherwise("Failure"))
        return df

    # Method to populate the migration objects DataFrame
    def populate_migration_objects_df(self, result_df_table, result_df_view, result_df_macro, result_df_procedure, result_df_udf, pass_number_value):
        # Union all DataFrames for different object types
        migration_objects_df = result_df_table.unionByName(result_df_view).unionByName(result_df_macro).unionByName(result_df_procedure).unionByName(result_df_udf)
        # Add columns 'pass_number', 'process', 'timestamp', and 'Result' and 'Catalog'
        migration_objects_df = self.featurise_migration_objects_df(migration_objects_df, pass_number_value, "ddl_extraction","")

        return migration_objects_df

    def rerun_failed_objects(self, pass_number):
        # Query to select failed objects from the migration history table
        query = f"""
        select db, object, object_type from {teradata_extractor.object_store_db}.{migration_table_name} where result = 'Failure' and pass_number = {pass_number-1}
        """

        failure_cases_df = spark.sql(query)

        return failure_cases_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teradata to DataBricks

# COMMAND ----------

# Create instances of the TeradataObjectExtraction, TeradataDDLExtractor, and Helper classes

# Instance of TeradataObjectExtraction class
teradata_extractor = TeradataObjectExtraction(db, auth, host, env, user, password, mht_db)

# Instance of TeradataDDLExtractor class
teradata_ddl_extractor = TeradataDDLExtractor(user, password, db, auth, host)

# Instance of Helper class
helper_instance = Helper()

# Create a pipeline instance using TeradataMigrationPipeline class
migration_pipeline = TeradataMigrationPipeline(inclusion_csv_file_path, helper_instance, teradata_ddl_extractor)

# COMMAND ----------

# Define schema for objects DataFrame
schema = StructType([
    StructField("db", StringType(), nullable=False),
    StructField("object", StringType(), nullable=False),
])

# Define the UDF (User-Defined Function) for extract_ddl
extract_ddl_udf = udf(teradata_ddl_extractor.extract_ddl, StructType([
    StructField("ddl_output", StringType()),
    StructField("error_message", StringType())
]))


# COMMAND ----------

# Create Database
teradata_extractor.create_database()

# COMMAND ----------

def main():
    # Step 1 : Decide flow of pipeline (generic , selective or failure rerun )
    ##Returns pass_number for each type of flow. Also, returns dataframe with objects if generic or inclusive, else returns an empty dataframe for the objects only.  
    df_table, df_view, df_macro, df_procedure, df_udf , pass_number = migration_pipeline.decide_flow()
    migration_objects_df = ""
    failure_cases_df = ""

    ##Check if we need to re-run only failed cases, else run generic or inclusive. Here's the exclusion list only applies for generic and inclusive use-cases    
    if pass_number !=0 and pass_number !=-1 and pass_number > 0 :

        failure_cases_df = migration_pipeline.rerun_failed_objects(pass_number)

        # Extract DDL information using a UDF -> returns updated dataframe with error_message and results(S/F)
        failure_cases_df = failure_cases_df.\
            withColumn("ddl_output_struct", extract_ddl_udf(failure_cases_df["db"], failure_cases_df["object"], failure_cases_df["object_type"]))

        # Create a result DDL DataFrame
        failure_cases_df = teradata_ddl_extractor.create_result_ddl_df(failure_cases_df)

        # Add columns 'pass_number', 'process', 'timestamp', and 'result' to dataframe
        migration_objects_df = migration_pipeline.featurise_migration_objects_df(failure_cases_df, pass_number, "ddl_extraction","")
        
    else :  
        # Step 2: Exclude objects (if specified) - Remove excluded objects
        df_table, df_view, df_macro, df_procedure, df_udf = migration_pipeline.exclusion(df_table, df_view, df_macro, df_procedure, df_udf)

        # Step 3: Generate DDLs
        result_df_table, result_df_view, result_df_macro, result_df_procedure, result_df_udf = migration_pipeline.fetch_ddl(df_table, df_view, df_macro, df_procedure, df_udf)

        # Step 4: Create migration_objects_df - Combine all objects data using union, Add columns 'pass_number', 'process', 'timestamp', and 'Result'
        migration_objects_df = migration_pipeline.populate_migration_objects_df(result_df_table, result_df_view, result_df_macro, result_df_procedure, result_df_udf , pass_number)

    # Step 5: Store DDL Extraction to MH table
    helper_instance.create_or_append_table_migration_history_table(migration_objects_df, teradata_extractor.object_store_db, migration_table_name)

# COMMAND ----------

if __name__ == "__main__" :
    main()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DISPLAY TABLES CREATED SUCCESSFULLY

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize hive_metastore.mht_demo_db.mht_table_test;
# MAGIC
# MAGIC select * from hive_metastore.mht_demo_db.mht_table_test limit 10;
# MAGIC
# MAGIC
