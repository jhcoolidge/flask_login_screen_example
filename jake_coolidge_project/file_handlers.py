import pandas


# Check if the file extension is able to be handled by the program.
def is_allowed_file(file_extension):
    return file_extension.lower() in ['csv', 'json']


# Read files given to the program
def read_file(file):
    file_dataframe = None
    file_extension = file.filename.split('.')[-1]

    if file_extension.lower() == "csv":
        file_dataframe = pandas.read_csv(file, on_bad_lines="skip")
    elif file_extension.lower() == "json":
        file_dataframe = pandas.read_json(file)

    if file_dataframe is not None and len(file_dataframe) >= 1:
        return file_dataframe

    raise ValueError("File appears to be empty!")
