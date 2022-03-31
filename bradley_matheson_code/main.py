#Project: Import json to bigquery table
#StartDate: 3/24/2022
#EndDate: 
#Developer: Bradley Matheson

from flask import Flask,render_template,request
from google.cloud import bigquery
from google.oauth2 import service_account
import json
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

#This will render the frontend of the website to get the json file
@app.route('/', methods=['GET','POST'])
def index():

    #resets the message so that previous messages dont confuse the user
    message = None

    #This will only run if the user attempts to submit a file
    if request.method == 'POST':

        #this will only run if what the user submitted is a file
        if 'file' in request.files: #Dont need, need to change to only allow files to be uploaded-----------
#Need to allow multiple files to be selected---------------

#Need to check if the file is empty------------------------

            #this gets the file data
            file = request.files['file']
            #this aquires the name of the file
            filename = secure_filename(file.filename)
            
            #this will only run if the file is a json
            if filename.endswith('.json'):
                
                #This will attempt to create a job to run to bigquerry
                try:
                    job_config = bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField('name', "STRING","REQUIRED"),
                            bigquery.SchemaField('id', "INTEGER","REQUIRED"),
                            bigquery.SchemaField('salary_in_k', "FLOAT","REQUIRED"),
                            bigquery.SchemaField('phonenumber', "STRING","REQUIRED")
                        ],
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    )

                    #This extracts the data out of the json
                    data = file.read()

                    #clean the data to get rid of formating comments
                    c_data = json.loads(data)

                    load_job = client.load_table_from_json(
                        c_data,
                        table_id,
                        job_config=job_config
                    )
                
                    #This will attempt to run the job
                    try:
                        load_job.result() 
                        message = "Json successfully uploaded"
                
                    #If the job running fails this will run instead letting the user know the json couldnt be uploaded
                    except Exception as error:
                        print('This was the error: ', error)
                        message = "This json file could not be uploaded"
                
                #If setting up the job failed, then this will run letting the user know that there was an error in the request
                except Exception as error:
                    print('This was the error: ', error)
                    message = "There was an error in creating the request"
            
            #this will only run if the file is a csv
            elif filename.endswith('.csv'):
                try:

                    #This will attempt to create a job to run to bigquerry
                    job_config = bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField('name', "STRING","REQUIRED"),
                            bigquery.SchemaField('id', "INTEGER","REQUIRED"),
                            bigquery.SchemaField('salary_in_k', "FLOAT","REQUIRED"),
                            bigquery.SchemaField('phonenumber', "STRING","REQUIRED")
                        ],
                        source_format=bigquery.SourceFormat.CSV,
                        max_bad_records=100
                    )
                   
                    #This extracts the data out of the csv
                    df = pd.read_csv(file)#need to change to go through the try block, store good records to bigquery, send bad records to notepad with why it was error and send google cloud storage---------------------

                    load_job = client.load_table_from_dataframe(
                        df,
                        table_id,
                        job_config=job_config
                    )
                
                    #This will attempt to run the job
                    try:
                        load_job.result() 
                        message = "Csv successfully uploaded"
                
                    #If the job running fails this will run instead letting the user know the csv couldnt be uploaded
                    except Exception as error:
                        print('This was the error: ', error)
                        message = "This csv file could not be uploaded"
                
                #If setting up the job failed, then this will run letting the user know that there was an error in the request
                except Exception as error:
                    print('This was the error: ', error)
                    message = "There was an error in creating the request"
            
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