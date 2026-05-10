######## Import Libraries ###########

from pyspark.sql.functions import col, to_timestamp, current_timestamp, try_to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, col, trim

######## Calling DBUtils ###########

dbutils.widgets.text("ref_id", "")
dbutils.widgets.text("config_catalog", "")
dbutils.widgets.text("config_schema", "")
dbutils.widgets.text("config_table", "")
# Step 1: Accept ref_id as a job parameter
ref_id_str = dbutils.widgets.get("ref_id")
config_catalog = dbutils.widgets.get("config_catalog")
config_schema = dbutils.widgets.get("config_schema")
config_table = dbutils.widgets.get("config_table")

######## Calling Ref_id from Config Table ###########

if not ref_id_str.isdigit():
    raise ValueError("Invalid or missing ref_id. Please pass a numeric ref_id as a parameter.")

ref_id = int(ref_id_str)

# Step 2: Fetch configuration from the config table
config_df = spark.table(f"{config_catalog}.{config_schema}.{config_table}").filter(f"ref_id = {ref_id}")

if config_df.count() == 0:
    raise ValueError(f"No configuration found for ref_id = {ref_id}")

######## Autoloader Read Stream (JSON) ###########

config = config_df.collect()[0]
input_path = config["input_path"]
file_format = config["file_format"]
schema_path = config["schema_path"]
#archive_path = config["archive_path"]
checkpoint_path = config["checkpoint_path"]
table_catalog = config["table_catalog"]
table_schema = config["table_schema"]
ingst_tbl_name = config["ingst_tbl_name"]
glob_filter = config["glob_filter"]
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
schema = StructType([
    StructField("TaskId", StringType(), True),
    StructField("TimeUpdated", StringType(), True),
    StructField("TimeCreated", StringType(), True),
    StructField("UserId", StringType(), True)
])

df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)              # "json"
        .option("cloudFiles.schemaLocation", schema_path)
        #.option("cloudFiles.schemaHints", schema_hint)         # 👈 force struct
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.validateOptions", "false")
        .option("pathGlobFilter", glob_filter)
        .schema(schema)
        .load(input_path)
)

######## Mapping Source to Target ###########

df_new = (
    df_raw.withColumnRenamed("TaskId", "taskid")
      .withColumnRenamed("TimeUpdated", "timeupdated")
      .withColumnRenamed("TimeCreated", "timecreated")
      .withColumnRenamed("UserId", "activeapprovers_userid")

)
df_new = df_new.withColumn("update_ts", current_timestamp())
df_new = df_new.drop("_rescued_data")
df_new = df_new.select(
    "taskid",
    "timecreated",
    "timeupdated",
    "activeapprovers_userid",
    "update_ts"
)

######## Truncate Table (Optional) ###########

spark.sql(f"TRUNCATE TABLE {table_catalog}.{table_schema}.{ingst_tbl_name}")

######## Autoloader Write Stream ###########

df_new.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .table(f"{table_catalog}.{table_schema}.{ingst_tbl_name}")
