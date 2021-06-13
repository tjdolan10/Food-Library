import sys, logging, os
from .controller import app as application
from waitress import serve
logging.basicConfig(stream=sys.stderr)


if __name__ == "__main__":
    port = os.environ.get('PORT',5000)
    serve(application, host="0.0.0.0", port=port)
