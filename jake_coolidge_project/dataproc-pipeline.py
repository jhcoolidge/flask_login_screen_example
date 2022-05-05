from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.types import _parse_datatype_string, _infer_type, StructType, StructField, IntegerType
from pyspark.sql.functions import isnull, col, length, round, monotonically_increasing_id
import datetime
import json
import os
import io

bq_client = bigquery.Client()

project_id = "regal-stage-343104"
dataset_id = "uploaded_data"
table_name = 'dataproc_upload'

dataset_ref = bq_client.get_dataset(dataset_id)
table_ref = dataset_ref.table(table_name)
table = bq_client.get_table(table_ref)

os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

# Get the time when the program started
start_time = datetime.datetime.now()

# -----------------------------------------------------------------------------------------
# SPARK SESSION SETUP
# -----------------------------------------------------------------------------------------
spark_session = SparkSession \
    .builder \
    .master("yarn") \
    .appName('pyspark-etl-example') \
    .getOrCreate()


def find_required_columns(schema):
    required_columns_list = []

    for field in schema:
        if field["mode"].lower() == "required":
            required_columns_list.append(field['name'])

    return required_columns_list


def add_error_message(df, schema):
    # Arrange variables needed by function.
    new_rows = []
    order = []
    fields = json.loads(schema.json())
    fields = fields["fields"]

    # Assign row numbers that do not change with the order of the rows.
    # NOTE: ROW_NUMBER() DOES NOT WORK HERE. THE NUMBERS WILL CHANGE BASED ON THE ORDER OF THE ROWS.
    for row_index, row in enumerate(df.collect()):
        row = row.asDict()
        row["_row_number"] = row_index
        new_rows.append(row)

    # Recast the data back into a dataframe with the row numbers as a new column
    _new_schema = StructType(df.schema.fields + [StructField("_row_number", IntegerType(), False)])
    df_with_row_numbers = spark_session.createDataFrame(new_rows, _new_schema)

    # Collect the new data to iterate through.
    collected_data = df_with_row_numbers.collect()

    # Append the headers in log file
    for field in fields:
        order.append(field['name'])
    order.append('error_message')

    # Empty the new rows variable for more rows.
    new_rows = []
    for row_index, row in enumerate(collected_data):
        # Convert from PySpark SQL Row to list of Dicts
        row = row.asDict()

        # Get the current row as a dataframe object to attempt to cast the fields to their respective data types
        datatype_check_row = df_with_row_numbers.where(df_with_row_numbers["_row_number"] == row_index)
        print(datatype_check_row)

        # Error message can contain multiple errors per row
        error_message = ""
        for field_index, field in enumerate(fields):
            # Get the current field's name
            column_name = field['name']

            # Find if row needs a value
            if not field['nullable'] and row[column_name] is None:
                error_message += "Error: Row has a null value at {} when column is required. "\
                    .format(column_name)

            # Skip for all None values, no point in doing below code for None
            if row[column_name] is not None:
                # Get the value before casting
                value_before_check = datatype_check_row.collect()[0].asDict()[column_name]

                # Cast the current column to what it should be
                casted_row = datatype_check_row.withColumn(column_name, col(column_name).cast(field["type"]))

                # Get the value after casting. If it is None, the casting failed.
                # NOTE: Floats will truncate their decimals if casted to Int and will not return None.
                value_checked = casted_row.collect()[0].asDict()[column_name]

                # Check if datatype casting has succeeded, which will only work for intended values
                if value_checked is None:
                    error_message += "Error: '{}' at column {} should be type {} but was type {} instead. "\
                        .format(row[column_name],
                                column_name,
                                _parse_datatype_string(field['type']),
                                _infer_type(type(row[column_name]).__name__))
                # Check if a float was changed at all as it will not return None, only truncate decimals
                elif str(value_checked).lower() != str(value_before_check).lower():
                    error_message += "Error: {} at column {} should be type Integer but was type Float instead. " \
                        .format(row[column_name],
                                column_name)

            # Convert the datatype back to string to help create dataframe again
            if row[column_name] is not None:
                row[column_name] = str(row[column_name])
            else:
                row[column_name] = ''

        # Append error message to the row dictionary
        row['error_message'] = error_message
        new_rows.append(row)

    # Create dataframe from list of Dicts
    bad_data_with_error = spark_session.createDataFrame(new_rows)
    bad_data_with_error = bad_data_with_error.select(order)
    bad_data_with_error.show()

    # Return the dataframe with error to be written to the log file.
    return bad_data_with_error


def limit_column_lengths(schema, file_dataframe):

    print(schema)
    for field in schema:
        if "maxLength" in field:
            print("found max length for field " + field["name"])
            file_dataframe = file_dataframe.where(length(col(field["name"])) <= field["maxLength"])

        # Not sure what precision refers to yet
        # if "precision" in field:
        #     file_dataframe = file_dataframe.where(col(field["name"]) >= 2 ** field["precision"])

        if "scale" in field:
            print("found scale for field" + field["name"])
            file_dataframe = file_dataframe.withColumn(field["name"], round(field["name"], field["scale"]))

    file_dataframe.show()
    return file_dataframe


f = io.StringIO("")

# Read the schema correctly. May not work correctly as Dataproc uses a different version of BigQuery connectors
bq_client.schema_to_json(table.schema, f)

# Convert the schema to JSON
data_schema = json.loads(f.getvalue())

# Find the required columns using the schema
required_columns = find_required_columns(data_schema)

# Use this bucket as the uploaded files will go here
bucket_name = "example-data-111999"

# Temporary file name until Cloud Functions update
file_name = "gs://{}/bad_data.csv".format(bucket_name)
name = file_name.split('/')[-1].split('.')[0]

# Below is REQUIRED for spark job to run on Dataproc cluster.
spark_session.conf.set('temporaryGcsBucket', bucket_name)

# Read the file data in with the given schema. Drop any rows that do not match the datatypes.
file_extension = file_name.split('.')[-1]
if file_extension == "csv":
    file_data = spark_session.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", False) \
        .schema(table.schema) \
        .option("columnNameOfCorruptRecord", "_corrupted_record") \
        .csv(file_name)
elif file_extension == "json":
    file_data = spark_session.read \
        .schema(table.schema) \
        .option("multiline", True)\
        .option("columnNameOfCorruptRecord", "_corrupted_record") \
        .json(file_name)
elif file_extension == "avro":
    file_data = spark_session.read\
        .format("com.databricks.spark.avro") \
        .option("inferSchema", False) \
        .schema(table.schema) \
        .option("columnNameOfCorruptRecord", "_corrupted_record") \
        .load(file_name)
else:
    print("File extension could not be handled by program. Supported extensions: csv, json, avro")
    exit()

# Create unique IDs for each row without changing the data.
file_data = file_data.withColumn("_idx", monotonically_increasing_id())

# Create a copy of the dataframe and add aliases to use in SQL type queries
file_data = file_data.alias("file_data")
bad_data = file_data.alias("bad_data")

# Drop rows which cannot be null in BQ. Can be configured for specific columns instead of any column.
file_data = file_data.dropna('any', subset=required_columns).rdd.toDF(table.schema)

# Limit string lengths and numeric/bignumeric precision and scale
file_data = limit_column_lengths(data_schema, file_data)

# Drop corrupted records
file_data = file_data.filter(col("_corrupt_record").isNull)

file_data.show()

# Get the number of good rows after processing out bad ones
num_good_rows = file_data.count()

# Write the good records to BQ and overwrite anything that is there.
file_data.write.format('bigquery') \
    .option('table', 'uploaded_data.{}'.format(table_name)) \
    .mode("overwrite") \
    .save()


# Join where the unique IDs are not present in the "Good rows" dataframe
bad_data = file_data.join(bad_data, on=file_data["_idx"] == bad_data["_idx"], how="right").where(isnull(file_data["_idx"])).select("bad_data.*")

# bad_data.show()

num_bad_rows = bad_data.count()

if num_bad_rows > 0:
    # Add the error_message column with everything the column has wrong
    bad_records = add_error_message(bad_data, schema=table.schema)

    # Get a timestamp for the folder containing the logs
    timestamp = datetime.datetime.now()
    readable_timestamp = timestamp.strftime("%d-%m-%Y-%H-%M-%S")
    log_file_path = "gs://{}/logs/{}-{}-{}".format(bucket_name, name, 'bad_records', str(readable_timestamp))

    # Coalesce and stop parallel writing of CSV files into one single file
    bad_records.coalesce(1).write \
        .format("com.databricks.spark.csv") \
        .option('header', True) \
        .csv(log_file_path)


# Print number of good rows, bad rows and total number of rows processed. There should not be any missing at any time
print("""
Total Num of Rows: {}
Num Good Rows: {}
Num Bad Rows : {}
""".format(str(num_bad_rows + num_good_rows), num_good_rows, num_bad_rows))


# Record time after execution
end_time = datetime.datetime.now()

# Print execution time in human readable format
print("The program took {} to execute.".format(end_time - start_time))
