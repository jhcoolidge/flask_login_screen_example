from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull
import datetime
import json
import sys

# -----------------------------------------------------------------------------------------
# SPARK SESSION SETUP
# -----------------------------------------------------------------------------------------
spark_session = SparkSession \
    .builder \
    .master("yarn") \
    .appName('pyspark-etl-example') \
    .getOrCreate()

# file_name = sys.argv[0]
# table_name = sys.argv[1]


def find_required_columns(schema):
    required_columns = []
    json_schema = json.loads(schema.json())

    for field in json_schema["fields"]:
        if not field['nullable']:
            required_columns.append(field['name'])
    return required_columns


def add_error_message(df, schema):
    collected_data = df.collect()
    new_rows = []
    order = []
    fields = json.loads(schema.json())
    fields = fields["fields"]

    field_types = {'long': int, 'string': str}

    for field in fields:
        order.append(field['name'])
    order.append('error_message')

    for row in collected_data:
        row = row.asDict()

        error_message = ""
        for field_index, field in enumerate(fields):
            column_name = field['name']

            if row[column_name] is not None:
                try:
                    if row[column_name].isdigit():
                        row[column_name] = int(row[column_name])
                    else:
                        row[column_name] = float(row[column_name])
                except ValueError:
                    pass

            if not field['nullable'] and row[column_name] is None:
                error_message += "Error: Row has a null value at {} when column is required. "\
                    .format(column_name)

            elif field_types[field['type']] != type(row[column_name]) and row[column_name] is not None:
                error_message += "Error: '{}' at column {} should be type {} but was type {} instead. "\
                    .format(row[column_name], column_name, field['type'], type(row[column_name]))

            if row[column_name] is not None:
                row[column_name] = str(row[column_name])
            else:
                row[column_name] = ''

        row['error_message'] = error_message
        new_rows.append(row)

    bad_data_with_error = spark_session.createDataFrame(new_rows)
    bad_data_with_error = bad_data_with_error.select(order)
    bad_data_with_error.show()

    return bad_data_with_error


table_name = 'dataproc_upload'

# Read the schema from the BigQuery table
data_schema = spark_session.read.format('bigquery').option('table', 'uploaded_data.{}'.format(table_name)).load()

required_columns = find_required_columns(data_schema.schema)

# TODO: Enter your bucket ID here
# Use this bucket as the uploaded files will go here
bucket_name = ""

# Temporary file name until Cloud Functions update
file_name = "gs://{}/bad_data.csv".format(bucket_name)
name = file_name.split('/')[-1].split('.')[0]


# Below is REQUIRED for spark job to run on Dataproc cluster.
spark_session.conf.set('temporaryGcsBucket', bucket_name)

# Read the file data in with the given schema. Drop any rows that do not match the datatypes.
file_data = spark_session.read \
              .format("csv") \
              .option("header", True) \
              .option("inferSchema", False) \
              .schema(data_schema.schema) \
              .option('mode', 'DROPMALFORMED') \
              .load(file_name)

# Drop rows which cannot be null in BQ. Can be configured for specific columns instead of any column.
file_data = file_data.dropna('any', subset=required_columns).rdd.toDF(data_schema.schema)

# file_data.show()

# Write the good records to BQ and overwrite anything that is there.
file_data.write.format('bigquery') \
    .option('table', 'uploaded_data.{}'.format(table_name)) \
    .mode("overwrite") \
    .save()

# Read the file but do not drop any records. Do not load pre-defined schema either.
bad_record_data = spark_session.read \
                  .format("csv") \
                  .option("header", True) \
                  .load(file_name)

# bad_record_data.show()

# Join the dataframes where the bad records emp id is NOT present in the good records dataframe.
# This assumes that emp id is a primary key and is therefore unique
bad_record_data = bad_record_data.alias("bad_record_data")
file_data = file_data.alias("file_data")

bad_records = file_data \
    .join(bad_record_data, on=file_data.emp_id == bad_record_data.emp_id, how="right") \
    .where(isnull(file_data.emp_id)) \
    .select('bad_record_data.*')

# bad_records.show()

# Add the error_message column with everything the column has wrong
bad_records = add_error_message(bad_records, schema=data_schema.schema)

# Get a timestamp for the folder containing the logs
timestamp = datetime.datetime.now()
readable_timestamp = timestamp.strftime("%d-%m-%Y-%H-%M-%S")
log_file_path = "gs://{}/logs/{}-{}-{}".format(bucket_name, name, 'bad_records', str(readable_timestamp))

# Coalesce and stop parallel writing of CSV files into one single file
bad_records.coalesce(1).write \
    .format("com.databricks.spark.csv") \
    .option('header', True) \
    .csv(log_file_path)
