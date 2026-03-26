from flask import Flask, request
import os

"""This script runs a flask server listening to the local host on port 8182 to receive callback url and return authentication code"""

codes = []

app = Flask(__name__)

@app.route("/")
def page() ->str:
    code = request.args.get("code")
    codes.append(code)
    return ''


dir_path = os.path.dirname(os.path.abspath(__file__))

def run_server()-> None:
    """ Runs HTTPS server with self-signed certificate"""
    app.run(ssl_context = (dir_path + '/config/certificates/cert.pem', dir_path + '/config/keys/key.pem'), port=8182, debug= False)

