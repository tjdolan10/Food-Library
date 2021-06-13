import os, sys, time
from flask import Flask
from flask_bootstrap import Bootstrap


app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFUALT"] = 0
app.config["TEMPLATES_AUTO_RELOAD"] = True
Bootstrap(app)

from .views import home

def main():
    app.run(debug=True, host="0.0.0.0", port=5000)

if __name__ == "__main__":
    main()
