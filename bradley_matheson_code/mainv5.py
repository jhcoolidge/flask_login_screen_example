#Project: Import json to bigquery table
#StartDate: 3/24/2022
#EndDate: 
#Developer: Bradley Matheson

#Need to allow multiple files to be selected---------------


from flask import Flask,render_template,request
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import datetime, re
from werkzeug.utils import secure_filename
import pandas as pd



#this is needed to run the website html
app = Flask(__name__)

#secret key is to allow this python code to move variable from the frontend to the backend
app.secret_key = "35367565"

#this is to allow a authenticated user on the project to work on bigquerry
Key_path = "C:\\Users\Brad\Documents\model-craft-342921-ea36cdb339e7.json"

#this is needed to request a job
credentials = service_account.Credentials.from_service_account_file(Key_path,scopes=["https://www.googleapis.com/auth/cloud-platform"])
client=bigquery.Client(credentials=credentials,project=credentials.project_id)
table_id='model-craft-342921.testing.Task2'
storage_client=storage.Client(credentials=credentials,project=credentials.project_id)
bucket = storage_client.get_bucket("practice_error_logs")



def get_error_report(errors, file_dataframe):
    report = []
    try:
        for error_index, error in enumerate(errors):
            try:
                error_value = re.findall("'\D*'", error['message'])[0][1:-1]
                found_error_records = file_dataframe[file_dataframe.isin([error_value]).any(axis=1)].values #doesnt find null errors, needs to be fixed--------------------
            except:
                found_error_records = 'null'
            log_string = """| Error # - {} || Error Message - {} || Inputted Data - {} |""".format(error_index, error['message'], found_error_records)
            report.append(log_string)
    except:
        raise
    return str(report)


#This will render the frontend of the website to get the json file
@app.route('/', methods=['GET','POST'])
def index():

    #resets the message so that previous messages dont confuse the user
    message = None

    empty = False

    #This will only run if the user attempts to submit a file
    if request.method == 'POST':

        #this will only run if what the user submitted is a file
        if 'file' in request.files:

            #this gets the file data
            file = request.files['file']
            #this aquires the name of the file
            filename = secure_filename(file.filename)
            if filename.endswith('.json') or filename.endswith('.csv'):

                #this will only run if the file is a json
                if filename.endswith('.json'):
                    try:
                        #This extracts the data out of the json
                        file_data = pd.read_json(file)
                    except:
                        empty = True

                #this will only run if the file is a csv
                elif filename.endswith('.csv'):
                    try:
                        #This extracts the data out of the csv
                        file_data = pd.read_csv(file)
                    except:
                        empty = True
                    
                 #endif

                if empty == False:
                    #This will attempt to create a job to run to bigquerry
                    try:
                        job_config = bigquery.LoadJobConfig(
                            schema=[
                                bigquery.SchemaField('name', "STRING","REQUIRED"),
                                bigquery.SchemaField('id', "INTEGER","REQUIRED"),
                                bigquery.SchemaField('salary_in_k', "FLOAT","REQUIRED"),
                                bigquery.SchemaField('phonenumber', "STRING","REQUIRED")
                            ],
                            source_format = bigquery.SourceFormat.CSV,
                            max_bad_records=100,
                        )

                        load_job = client.load_table_from_dataframe(
                                file_data,
                                table_id,
                                job_config=job_config
                            )
                        #This will attempt to run the job
                        try:
                            load_job.result() 

                            filename = "LOG: "+ str(datetime.datetime.now()) + ".txt"
                            if load_job.errors:
                                blob = bucket.blob(filename)
                                #log = str(load_job.errors)
                                log = get_error_report(load_job.errors,file_data)
                                blob.upload_from_string(log)
                                message = "File successfully uploaded, but there were a few bad records"
                            else:
                                message = "File successfully uploaded"

                    
                        #If the job running fails this will run instead letting the user know the json/csv couldnt be uploaded
                        except Exception as error:
                            print('This was the error: ', error)
                            message = "This file could not be uploaded"
                    
                    #If setting up the job failed, then this will run letting the user know that there was an error in the request
                    except Exception as error:
                        print('This was the error: ', error)
                        message = "There was an error in creating the request"
                else:
                    message = "This file has no data to process"
                #endif
    
            #If the file is not a json or the csv this will run
            else:
                message = "File type is not excepted"
            #endif

        #This will run if the submition is not a file type
        elif 'file' not in request.files:
            message = "There was no file to upload"
        #endif
    #endif

    #this will render the template on the website
    return render_template("front.html", message = message)

if __name__ == '__main__':
    app.run()