######## Import Libraries ###########

import requests
import json
import time
from datetime import datetime
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import to_timestamp, current_timestamp, try_to_timestamp
from pyspark.sql.functions import current_timestamp
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import requests
import json
import time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, DecimalType, LongType
from pyspark.sql.functions import to_timestamp, current_timestamp, to_date, date_format, col, when, expr, current_date
from pyspark.sql import functions as F

######## Calling DBUtils ###########

overall_start_time = datetime.now()
# COMMAND ----------
# Widgets for environment and object name
dbutils.widgets.text("env", "", "Environment")
dbutils.widgets.text("object_name", "sds_mdx_device", "Object_Name")
env = dbutils.widgets.get("env")
object_name = dbutils.widgets.get("object_name")

######## Calling Databricks Secrets ###########

dbutils.widgets.text("password_scope","pass_scope","Password Scope")
dbutils.widgets.text("password_key","password","Password Key")
dbutils.widgets.text("username_key","username","Username Key")
password_scope = dbutils.widgets.get("password_scope")
password_key = dbutils.widgets.get("password_key")
username_key = dbutils.widgets.get("username_key")
user_name=dbutils.secrets.get(scope=password_scope, key=username_key)
pass_word = dbutils.secrets.get(scope=password_scope, key=password_key)

######## Calling object from Config Table ###########

# Fetch configuration for the given object
config_df = spark.table(f"{env}_sdh_db.framework_config.sdh_api_src_config") \
    .filter(f"object_name = '{object_name}'") \
    .limit(1)
if config_df.isEmpty():
    raise ValueError(f"No configuration found for the object = {object_name}")

######## Setting values in Variable ###########

config = config_df.first()
object_id = config["object_id"]
apiurl = config["source_url"]
source_name = config["source_name"].lower()
object_name = config["object_name"].lower()
UPDATE_COL_NAME = config["source_delta_column"]
print("Api Url:", apiurl)
username = f"{user_name}"
password = f"{pass_word}"
api_url = f"{apiurl}"

######## Audit Logging  ###########

def log_audit(run_id, object_id, job_id, object_name, source_name, job_name, notebook_name, latest_update_dt, 
              start_time=None, end_time=None, run_status=None, log_step=None):
    duration = (end_time - start_time).total_seconds() if start_time and end_time else None
    audit_data = [(run_id, object_id, job_id, object_name, source_name, job_name, notebook_name,
                   latest_update_dt, start_time, end_time,
                   str(duration) if duration else None, run_status, log_step, datetime.now())]
    audit_schema = StructType([
        StructField("run_id", StringType(), True),
		StructField("object_id", IntegerType(), True),
		StructField("job_id", StringType(), True),
		StructField("object_name", StringType(), True),
        StructField("source_name", StringType(), True),
		StructField("job_name", StringType(), True),
        StructField("notebook_name", StringType(), True),
        StructField("latest_update_dt", TimestampType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_sec", StringType(), True),
		StructField("run_status", StringType(), True),
		StructField("log_step", StringType(), True),
        StructField("audit_ts", TimestampType(), True)
    ])
    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
    audit_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(
        f"{env}_sdh_db.framework_config.sdh_api_src_audit_log"
    )

get_job_name_df = spark.sql("""
    select job_id as job_id, name as job_name from system.lakeflow.jobs where tags['layer'] = 'SDH' and tags['schema'] = 'MDX' and tags['object'] = 'DEVICE_CDC' limit 1
""")
job_name = get_job_name_df.first()["job_name"] if not get_job_name_df.isEmpty() else None
job_id = get_job_name_df.first()["job_id"] if not get_job_name_df.isEmpty() else None
notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name = notebook.split("/")[-1]

######## Defining target table variable ###########

# Define your environment and table names
target_table = f"<catalog>.<schema>.<table>"
audit_table = f"<catalog>.<schema>.<table>"
# Set the API

######## Defining modified date for Change Data Capture (CDC) ###########

# Step 1: Get the latest modified_date from the target table
latest_modified_date_df = spark.sql(f"""
    SELECT MAX(latest_update_dt) AS modified_date
    FROM {audit_table}
    where  object_name = "{object_name}" and source_name = "{source_name}" and upper(run_status) = 'SUCCESS'
""")

latest_modified_date = latest_modified_date_df.collect()[0]["modified_date"]
print(latest_modified_date)


if isinstance(latest_modified_date, datetime):
    dt = latest_modified_date
else:
    dt = datetime.strptime(
        str(latest_modified_date), "%m/%d/%Y"
    )

latest_modified_date_str = dt.strftime("%m/%d/%Y %H:%M:%S")

######## Calling API ###########

def safe_request(method, url, headers=None, params=None, retries=3):
    for attempt in range(retries):
        try:
            print(f"API Call Attempt {attempt + 1}: {url} | Params: {params}")
            response = requests.request(
                method, url, headers=headers, params=params,
                auth=(username, password), timeout=120
            )
            
            if response.status_code == 401:
                print("API Call Failed - Status Code: 401 Unauthorized. "
                      "Please check credentials or token.")
                # You can choose to return None or raise; raising is often clearer:
                raise RuntimeError("Unauthorized (401)")


            if response.status_code == 200:
                return response
            elif response.status_code == 204:
                return None
            else:
                print(f"API Call Failed - Status Code: {response.status_code}")
                time.sleep(2 ** attempt)
        except Exception as e:
            last_exception = e
            print(f"Atempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)
            overall_end_time = datetime.now()
            latest_update_dt = None
            log_audit(run_id, object_id, job_id, object_name, source_name, job_name, notebook_name, latest_update_dt, overall_start_time, overall_end_time, "FAILURE", log_step="INCORRECT USERNAME OR PASSWORD")
    
    # If we reach here, all attempts failed
    if last_exception:
        raise last_exception
    raise RuntimeError("API call failed after retries")    
#raise Exception(f"Failed after {retries} retries: {url}")
# COMMAND ----------
# Main execution with pagination and logging

run_id = str(uuid.uuid4())

try:
    # Initialize pagination variables
    limit = 1000  # configurable
    Page = 1
    modified_date = latest_modified_date_str
    all_device_data = []

    # Step: API Data Load
    step_start = datetime.now()

    while True:
        params = {
            "limit": limit,
            "Page": Page,
            "modifiedDate": modified_date
        }
        response = safe_request("GET", api_url, params=params)
        if response is None:
            print("No records found.")
            break
        if response is not None:
            data = response.json()
            device_data = data.get("deviceResponseData", [])
            # Stop pagination if current page has no data
            if not device_data:
                print(f"No data found on page {Page}. Stopping pagination.")
                break

            # Append data from current page
            all_device_data.extend(device_data)
            print(f"Fetched {len(device_data)} records from page {Page}")

            # Increment page for next iteration
            Page += 1

            # Define schema based on new structure
            # Define schema with all ID fields as StringType
            schema = StructType([
                StructField("status", StringType(), True),
                StructField("deviceStatus", StringType(), True),
                StructField("mendixUID", StringType(), True),
                StructField("mendixPID", StringType(), True),
                StructField("submissionSource", StringType(), True),
                StructField("submitterFirstname", StringType(), True),
                StructField("submitterLastname", StringType(), True),
                StructField("serialNumber", StringType(), True),
                StructField("modelNumber", StringType(), True),
                StructField("reasonForMonitoring", StringType(), True),
                StructField("monitorSerialNumber", StringType(), True),
                StructField("monitorModelNumber", StringType(), True),
                StructField("enrolledStatus", StringType(), True),
                StructField("procedureDate", StringType(), True),
                StructField("procedureAccount", StringType(), True),
                StructField("followupAccount", StringType(), True),
                StructField("procedureProviderNPI", StringType(), True),
                StructField("followupProviderNPI", StringType(), True),
                StructField("inputDate", StringType(), True),
                StructField("modifiedDate", StringType(), True),
                StructField("mendixEntityMergeID", StringType(), True),  
                StructField("GC_MX_ID", StringType(), True),             
                StructField("GC_RGSTRN_MX_UID", StringType(), True),    
                StructField("dateOfOldestTransmission", StringType(), True),
                StructField("enrollmentDate", StringType(), True),
                StructField("isResolving", BooleanType(), True),
                StructField("DateResolving", StringType(), True),
                StructField("RegPostedDate", StringType(), True),
                StructField("IsAutoCompleted", BooleanType(), True),
                StructField("completedDate", StringType(), True),
                StructField("deviceCategory", StringType(), True),
                StructField("assignedTo", StringType(), True)
            ])

            df = spark.createDataFrame(device_data, schema=schema).toDF(
                'status',
                'device_status',
                'mendix_uid',
                'patient_id',
                'submission_source',
                'submitter_first_name',
                'submitter_last_name',
                'serial_number',
                'model_number',
                'reason_for_monitoring',
                'monitor_serial_number',
                'monitor_model_number',
                'enrolled_status',
                'procedure_date',
                'procedure_account',
                'followup_account',
                'procedure_provider_npi',
                'followup_provider_npi',
                'input_date',
                'modified_date',
                'entity_merge_id',
                'gc_mendix_uid',
                'gc_rgstrn_mx_uid',
                'oldest_transmission_date',
                'enrollment_date',
                'isresolving',
                'resolving_date',
                'reg_posted_date',
                'isautocompleted',
                'completeddate',
                'devicecategory',
                'assignedto'
            )
            
            # List of columns to convert to timestamp
            df=df.withColumn("update_ts", current_timestamp())
            timestamp_columns = [
                "input_date", "modified_date", "reg_posted_date", "oldest_transmission_date", "enrollment_date", "resolving_date"
            ]
            date_columns = [
                "procedure_date",  "completeddate"
            ]

            # Convert timestamp columns using try_to_timestamp
            for col_name in timestamp_columns:
                df = df.withColumn(
                    col_name, to_timestamp(col_name, "MM/dd/yyyy HH:mm:ss")#, "MM/dd/yyyy HH:mm:ss")
                )
            for col_name in date_columns:
                df = df.withColumn(
                    col_name, try_to_timestamp(col_name)#, "MM/dd/yyyy HH:mm:ss")
                )

            

            
            # The following lines should NOT be indented
            df.createOrReplaceTempView("temp_view")

            #df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{env}_sdh_db.mdx.provider")
            merge_columns = [
                "mendix_uid", "patient_id", "serial_number", "model_number", "procedure_account", "followup_account", "procedure_provider_npi", "followup_provider_npi", "monitor_serial_number", "monitor_model_number", "procedure_date", "input_date", "device_status", "enrolled_status", "modified_date", "entity_merge_id", "status", "reason_for_monitoring", "submission_source", "submitter_first_name", "submitter_last_name", "gc_mendix_uid", "gc_rgstrn_mx_uid", "oldest_transmission_date", "enrollment_date", "isresolving", "resolving_date", "reg_posted_date", "isautocompleted", "devicecategory", "completeddate", "assignedto" 
            ]

            update_condition = " OR ".join([
                f"NOT (target.{col} <=> source.{col})" for col in merge_columns
            ])

            update_set = ",\n    ".join([f"target.{col} = source.{col}" for col in merge_columns])
            update_set += ",\n    target.update_ts = current_timestamp()"

            insert_cols = ", ".join(merge_columns + ['update_ts'])
            insert_vals = ", ".join([f"source.{col}" for col in merge_columns] + ['current_timestamp()'])
            
            ######## Merge into ###########
            
            spark.sql(f"""
                    MERGE INTO {target_table} AS target
                    USING temp_view AS source
                    ON target.mendix_uid = source.mendix_uid
                    WHEN MATCHED AND (
                        {update_condition}
                    ) THEN UPDATE SET
                        {update_set}
                    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                """)
                

    step_end = datetime.now()
    overall_end_time = datetime.now()
    tgt_after_df = spark.table(target_table)
    if UPDATE_COL_NAME.lower() in [c.lower() for c in tgt_after_df.columns]:
        # Column exists (case-insensitive): compute max
        latest_update_dt = tgt_after_df.agg(F.max(F.col(UPDATE_COL_NAME))).collect()[0][0]
        print(f"latest_update_dt (max({UPDATE_COL_NAME})) = {latest_update_dt}")
    else:
        print(f"⚠️ Column '{UPDATE_COL_NAME}' not found on target table; latest_update_dt will be NULL in audit.")
    
    # Overall success log
    log_audit(run_id, object_id, job_id, object_name, source_name, job_name, notebook_name, latest_update_dt, overall_start_time, overall_end_time, "SUCCESS", log_step="CDC_OVERALL_EXECUTION")
except Exception as e:
    overall_end_time = datetime.now()
    latest_update_dt = None
    log_audit(run_id, object_id, job_id, object_name, source_name, job_name, notebook_name, latest_update_dt, overall_start_time, overall_end_time, "FAILURE", log_step="CDC_OVERALL_EXECUTION")
    raise

print(latest_modified_date_str)

######## END ###########
