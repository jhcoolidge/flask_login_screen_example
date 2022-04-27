import json

from google.oauth2 import service_account
from google.cloud import bigquery
from dask.distributed import Client
import dask.dataframe as dd
import io
import os

key_path = 'C:\\Users\\jhcoo\\PycharmProjects\\FlaskLoginScreen\\bigquerykey.json'
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

project_id = "regal-stage-343104"
dataset_id = "uploaded_data"
table_id = "dataproc_upload"

dataset_ref = bq_client.get_dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
table = bq_client.get_table(table_ref)

os.environ["GOOGLE_CLOUD_PROJECT"] = project_id


def main():

    f = io.StringIO("")
    bq_client.schema_to_json(table.schema, f)
    print(json.loads(f.getvalue()))
    print(table.schema)

    df = dd.read_csv("C:\\Users\\jhcoo\\PycharmProjects\\FlaskLoginScreen\\big_data.csv")

    print(df.head())
    print(len(df))
    print(df.dtypes)


if __name__ == "__main__":
    client = Client(n_workers=4)
    main()
