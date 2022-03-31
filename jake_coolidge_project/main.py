try:
    from flask import Flask, render_template, request
    from bcrypt import hashpw, gensalt, checkpw
    from db_functions import store_user_pwd, get_user_pwd, upload_data_from_file
    from file_handlers import *
    import re
except ImportError as import_error:
    print("There was an error importing some packages.")
    print(import_error)
    exit(-1)

# -----------------------------------------------------------------------------------------
# SETUP FLASK APP
# -----------------------------------------------------------------------------------------

# Set up Flask App variables
app = Flask(
    __name__, static_url_path="", static_folder="static", template_folder="templates"
)

# This regex will validate any valid email addresses.
email_validate_regex = re.compile("([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+")

# These will ensure a password is at least 8 characters long,
# and has at least one digit, lower/uppercase and a special character
strong_password_regex = re.compile("((?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*]).{6,20})")

# -----------------------------------------------------------------------------------------
# APP ROUTES
# -----------------------------------------------------------------------------------------


# Displays the login form
@app.route("/")
def index():
    return render_template("login_form.html")


# Displays the Upload file form
@app.route('/upload_form')
def display_upload_form():
    return render_template("upload_file.html")


@app.route('/upload/file', methods=["POST"])
def handle_uploaded_file():
    message = ""

    file = request.files["file"]

    if file:
        message = upload_data_from_file(file=file)
    else:
        message = "File not uploaded or table name empty."

    # Re-render the template from before with the message
    return render_template("upload_file.html", message=message)


@app.route("/login/auth", methods=["POST"])
def login():
    # Extract user inputted username and password
    username = request.form['username']
    pwd = request.form['pwd']

    # Ensure the username is a valid email
    if re.fullmatch(email_validate_regex, username):
        # Ensure the inputted password is strong enough
        if re.fullmatch(strong_password_regex, pwd):

            # Retrieve the stored user password, if any
            stored_pwd_hash = get_user_pwd(username)

            # If there is already a user in the DB
            if stored_pwd_hash:
                # Check stored password hash against inputted password
                if checkpw(pwd.encode('utf-8'), stored_pwd_hash):
                    return render_template("login_success.html")
                message = "Invalid username or password!"

            # There is not already a user in the DB
            else:
                # Store the user's information as it fits the above policies
                store_user_pwd(username, hashpw(pwd.encode('utf-8'), gensalt()))
                username = ''
                message = "New user created!"
        else:
            message = "Password is not strong enough! It has to be 8 characters long at least, " \
                  "with at least 1 digit, a lower and uppercase letter and a special character!"
    else:
        message = "Invalid email address!"

    # Login failed somehow, render form with message explaining why
    return render_template("login_form.html", message=message, username=username)


if __name__ == '__main__':
    app.env = "development"
    app.debug = True
    app.run()
