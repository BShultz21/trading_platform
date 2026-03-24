import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from requests import HTTPError, post
import auth_server
import threading
import time
import base64
import json


class SchwabAPICredentials(object):
    def __init__(self) -> None:
        self.appKey = None
        self.secretKey = None
        self.callbackUrl = None
        self.encodedCredentials = ''
        self.authUrl = 'https://api.schwabapi.com/v1/oauth/authorize' + f'?client_id={self.appKey}&redirect_uri={self.callbackUrl}'
        self.tokenUrl = 'https://api.schwabapi.com/v1/oauth/token'
        self.authCode = None
        self.tokens = {'Access': {'Token': None, 'Timestamp': None}, 'Refresh': {'Token': None, 'Timestamp': None}}
        self.json = dict()
        self.token_file = 'config/tokens/token.json'

    def set_auth_url(self) -> None:
        self.authUrl = 'https://api.schwabapi.com/v1/oauth/authorize' + f'?client_id={self.appKey}&redirect_uri={self.callbackUrl}'

    def get_environment_variables(self):
        load_dotenv()
        self.appKey = os.getenv('APP_KEY')
        self.secretKey = os.getenv('SECRET_KEY')
        self.callbackUrl = os.getenv('callback_url')

    def write_token_data(self) -> None:
        """
        Writes access and refresh token data to a .json file with the token information and timestamp
        """
        try:
            with open(self.token_file, 'r+') as f:
                data = f.read()
                if data == '' or data == {}:
                    raise ValueError
                f.seek(0)
                f.write(json.dumps(self.tokens))
                f.truncate()

        except FileNotFoundError or ValueError:
            with open(self.token_file, 'w') as f:
                f.seek(0)
                f.write(json.dumps(self.tokens))
                f.truncate()

    def load_token_data(self) -> None:
        """
        Opens a .json file and saves data in class variable
        """
        try:
            with open(self.token_file) as f:
                data = f.read()
                if data == {} or data == '':
                    self.tokens = None
                else:
                    self.tokens = json.loads(data)
        except FileNotFoundError:
            self.tokens = None
            return None

    def check_for_valid_refresh_token(self) -> bool:
        """
        Takes token_data determines if the refresh token is valid
        """
        if self.tokens['Refresh']['Token'] and self.tokens['Refresh']['Timestamp']:
            current_time = datetime.now()
            token_time = self.tokens['Refresh']['Timestamp'][:16]
            token_time = datetime.strptime(token_time, "%Y-%m-%d %H:%M")
            delta = current_time - token_time
            d = timedelta(days=7)
            if delta < d:
                return True
            else:
                return False
        else:
            return False

    def check_for_valid_access_token(self) -> bool:
        """
        Takes token_data and determines if the access token is greater than 30 minutes old
        """
        if self.tokens['Access']['Token'] and self.tokens['Access']['Timestamp']:
            current_time = datetime.now()
            token_time = self.tokens['Access']['Timestamp'][:16]
            token_time = datetime.strptime(token_time, "%Y-%m-%d %H:%M")
            delta = current_time - token_time
            d = timedelta(minutes = 30)
            if delta < d:
                return True
            else:
                return False
        else:
            return False

    def get_auth_code(self) -> str:
        """
        Takes the query returned from authentication server and parses it to return authentication code
        """
        print(self.authUrl)
        threading.Thread(target=auth_server.run_server, daemon=True).start()
        while not self.authCode:
            if auth_server.codes:
                self.authCode = auth_server.codes[-1]
            time.sleep(3)

        return self.authCode

    def encode_credentials(self) -> str:
        """
        Encodes the client key and secret key provided by api in base64 ascii
        """
        self.encodedCredentials = self.appKey + ':' + self.secretKey
        self.encodedCredentials = base64.b64encode(self.encodedCredentials.encode('ascii')).decode('ascii')
        return self.encodedCredentials

    def get_json(self, auth_code) -> dict:
        """
        Takes authorization code, encoded credentials(client and secret key), and callback url to make post request
        to return json from authentication server
        """
        data = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.callbackUrl
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {self.encode_credentials()}"
        }
        response = post(self.tokenUrl, data=data, headers=headers)
        if response.status_code == 200:
            self.json = response.json()
            return self.json
        else:
            print(f'Error reaching Schwab API, server response code: {response.status_code}')
            raise HTTPError

    def get_tokens(self) -> None:
        """
        Parses a .json object to return access token
        """
        self.tokens['Access']['Token'] = self.json['access_token']
        date_time = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
        self.tokens['Access']['Timestamp'] = date_time

        self.tokens['Refresh']['Token'] = self.json['refresh_token']
        date_time = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
        self.tokens['Refresh']['Timestamp'] = date_time


    def use_refresh_token(self) -> None:
        """
        Uses refresh token to return new access token
        """
        request_data = {
        "grant_type": "refresh_token",
        "refresh_token": self.tokens['Refresh']['Token'],
        "redirect_uri": self.callbackUrl
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {self.encodedCredentials}"
        }

        response = post(self.tokenUrl, data = request_data, headers = headers)

        try:
            response.raise_for_status()
        except HTTPError:
            message = response.text
            print(f"HTTP Error message: {message}  Status code: {response.status_code}")
            #Need to call authcode
            raise
        data = response.json()
        self.tokens['Access']['Token'] = data["access_token"]
        date_time = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
        self.tokens['Access']['Timestamp'] = date_time

    def token_handler(self):
        #Need to modify to make sure that access token never expires via automatic use of refresh token

        self.load_token_data()
        if self.check_for_valid_access_token():
            return True
        elif self.check_for_valid_refresh_token():
            self.use_refresh_token()
            self.write_token_data()
            return True
        else:
            self.get_json(self.get_auth_code())
            self.get_tokens()
            self.write_token_data()
            return True


if __name__ == '__main__':
    load_dotenv()
    Schwab = SchwabAPICredentials()
    Schwab.get_environment_variables()
    Schwab.set_auth_url()
    Schwab.encode_credentials()
    Schwab.token_handler()