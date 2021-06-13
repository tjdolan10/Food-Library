from ...controller import app


@app.route("/")
def index():
    return "Is this working"
