######## Import Libraries ###########

from pyspark.sql.functions import col, to_timestamp, current_timestamp, try_to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, col, trim

######## Calling DButils ###########

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

######## Autoloader Read Stream JSON array ###########

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
schema_hint = (
    "approvalFactDelegatee ARRAY<STRUCT<"
    "SourceSystem: STRING, "
    "UserId: STRING, "
    "ApprovableId: STRING, "
    "TimeUpdated: STRING, "
    "Approver: STRING, " 
    "ApprovalActivationDate: STRING >>"
)

# Step 3: Configure and run Auto Loader
df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)              # "json"
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaHints", schema_hint)         # 👈 force struct
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.validateOptions", "false")
        .option("pathGlobFilter", glob_filter)
        .load(input_path)
)
df_23 = df.select(
    col("approvalFactDelegatee.SourceSystem").alias("SourceSystem"),
    col("approvalFactDelegatee.UserId").alias("UserId"),
    col("approvalFactDelegatee.ApprovableId").alias("ApprovableId"),
    col("approvalFactDelegatee.TimeUpdated").alias("TimeUpdated"),
    col("approvalFactDelegatee.Approver").alias("Approver"),
    col("approvalFactDelegatee.ApprovalActivationDate").alias("ApprovalActivationDate")
).withColumn("update_ts", current_timestamp())
from pyspark.sql.functions import arrays_zip, explode
df_flat = df_23.select(
    explode(
        arrays_zip(
            "SourceSystem",
            "UserId",
            "ApprovableId",
            "TimeUpdated",
            "Approver",
            "ApprovalActivationDate"
        )
    ).alias("zipped"),
    "update_ts"
)
df_table = df_flat.select(
    "update_ts",
    col("zipped.SourceSystem").alias("SourceSystem"),
    col("zipped.UserId").alias("UserId"),
    col("zipped.ApprovableId").alias("ApprovableId"),
    col("zipped.TimeUpdated").alias("TimeUpdated"),
    col("zipped.Approver").alias("Approver"),
    col("zipped.ApprovalActivationDate").alias("ApprovalActivationDate")
)

######## Mapping Source to Target ###########

df_new = (
    df_table.withColumnRenamed("ApprovableId", "approvableid")
      .withColumnRenamed("Approver", "approver")
      .withColumnRenamed("ApprovalActivationDate", "approvalactivationdate")
      .withColumnRenamed("TimeUpdated", "timeupdated")
      .withColumnRenamed("SourceSystem", "sourcesystem")
      .withColumnRenamed("UserId", "userid")
)
df_new = df_new.select(
    "approvableid",
    "approver",
    "approvalactivationdate",
    "timeupdated",
    "sourcesystem",
    "userid",
    "update_ts"
)

######## Truncate Table (Optional)###########

spark.sql(f"TRUNCATE TABLE {table_catalog}.{table_schema}.{ingst_tbl_name}")

######## Autoloader Write Stream ###########

df_new.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .table(f"{table_catalog}.{table_schema}.{ingst_tbl_name}")
