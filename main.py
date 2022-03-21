try:
    from flask import Flask, render_template, request
    from bcrypt import hashpw, gensalt, checkpw
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import re
except ImportError as import_error:
    print("There was an error importing some packages.")
    print(import_error)
    exit(-1)

# -----------------------------------------------------------------------------------------
# SETUP BIGQUERY AND FLASK APP
# -----------------------------------------------------------------------------------------

# Set up path to bigquery credential file and load the service account from it
key_path = 'C:\\Users\\jhcoo\\PycharmProjects\\FlaskLoginScreen\\bigquerykey.json'
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Set up BQ Client with given creds
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Set up Flask App variables
app = Flask(
    __name__, static_url_path="", static_folder="static", template_folder="templates"
)

# This regex will validate any valid email addresses.
email_validate_regex = re.compile("([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+")

# These will ensure a password is at least 8 characters long,
# and has at least one digit, lower/uppercase and a special character
strong_password_regex = re.compile("((?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*]).{6,20})")


@app.route("/")
def index():
    return render_template("login_form.html")


@app.route("/login/auth", methods=["POST"])
def check_credentials():
    username = request.form['username']
    pwd = request.form['pwd']

    if re.fullmatch(email_validate_regex, username):
        if re.fullmatch(strong_password_regex, pwd):

            stored_pwd_hash = get_user_pwd(username)

            if stored_pwd_hash:
                if checkpw(pwd.encode('utf-8'), stored_pwd_hash):
                    return render_template("login_success.html")
                message = "Invalid username or password!"
            else:
                store_user_pwd(username, hashpw(pwd.encode('utf-8'), gensalt()))
                message = "New user created!"
        else:
            message = "Password is not strong enough! It has to be 8 characters long at least, " \
                  "with at least 1 digit, a lower and uppercase letter and a special character!"
    else:
        message = "Invalid email address!"

    return render_template("login_form.html", message=message, username=username)


def get_user_pwd(username):
    query = "SELECT * FROM `regal-stage-343104.users.user_login` WHERE username = ? LIMIT 1;"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "STRING", username)
        ]
    )

    try:
        result = client.query(query, job_config=job_config)

        for row in result:
            return row["password"]
    except Exception as e:
        print("There was an issue retrieving user's password. Error: " + str(e))

    return None


def store_user_pwd(username, hashed_pwd):
    query = "INSERT INTO `regal-stage-343104.users.user_login` VALUES (?, ?);"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "STRING", username),
            bigquery.ScalarQueryParameter(None, "BYTES", hashed_pwd),
        ]
    )

    try:
        client.query(query, job_config=job_config)
    except Exception as e:
        print("There was a problem saving the user's password. " + str(e))
        return False
    return True


if __name__ == '__main__':
    app.env = "development"
    app.debug = True
    app.run()
