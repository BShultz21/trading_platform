from typing import Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from requests import HTTPError, post
from auth import auth_server
import threading
import time
import base64
import json
import os
from pathlib import Path

dir_path = os.path.dirname(os.path.abspath(__file__))


class SchwabAPICredentials(object):
    def __init__(self) -> None:
        self.appKey = None
        self.secretKey = None
        self.callbackUrl = None
        self.encodedCredentials = None
        self.authUrl = 'https://api.schwabapi.com/v1/oauth/authorize' + f'?client_id={self.appKey}&redirect_uri={self.callbackUrl}'
        self.tokenUrl = 'https://api.schwabapi.com/v1/oauth/token'
        self.authCode = None
        self.tokens: dict[str, dict[str, Optional[str]]] = {'Access': {'Token': None, 'Timestamp': None}, 'Refresh': {'Token': None, 'Timestamp': None}}
        self.token_file = dir_path + '/config/tokens/token.json'

    def set_auth_url(self) -> None:
        self.authUrl = 'https://api.schwabapi.com/v1/oauth/authorize' + f'?client_id={self.appKey}&redirect_uri={self.callbackUrl}'

    def get_environment_variables(self):
        if load_dotenv():
            self.appKey = os.getenv('APP_KEY')
            self.secretKey = os.getenv('SECRET_KEY')
            self.callbackUrl = os.getenv('callback_url')
        else:
            print("failed to load .env file")
            raise FileNotFoundError

    def write_token_data(self) -> None:
        """
        Writes access and refresh token data to a .json file with the token information and timestamp
        """
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
                    self.tokens = {'Access': {'Token': None, 'Timestamp': None}, 'Refresh': {'Token': None, 'Timestamp': None}}
                else:
                    self.tokens = json.loads(data)
        except FileNotFoundError:
            self.tokens = {'Access': {'Token': None, 'Timestamp': None}, 'Refresh': {'Token': None, 'Timestamp': None}}
            return None

    def check_for_valid_refresh_token(self) -> bool:
        """
        Determines if refresh token stored in self.tokens is greater than 7 days old
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
        Determines if access token stored in self.tokens if greater than 30 minutes old
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

    def get_auth_code(self) -> None:
        """
        Starts auth_server.py and takes the query returned from authentication server and sets self.authcode
        """
        print(self.authUrl)
        threading.Thread(target=auth_server.run_server, daemon=True).start()
        while not self.authCode:
            if auth_server.codes:
                self.authCode = auth_server.codes[-1]
            time.sleep(3)


    def encode_credentials(self) -> None:
        """
        Encodes the client key and secret key provided by api in base64 ascii
        """
        self.encodedCredentials = self.appKey + ':' + self.secretKey
        self.encodedCredentials = base64.b64encode(self.encodedCredentials.encode('ascii')).decode('ascii')

    def get_schwab_tokens(self) -> None:
        """
        Takes authorization code, encoded credentials(client and secret key), and callback url to make post request
        to return tokens in .json format from authentication server
        """
        if not self.encodedCredentials:
            self.get_environment_variables()
            self.encode_credentials()
            self.set_auth_url()
        self.get_auth_code()

        data = {
            "grant_type": "authorization_code",
            "code": self.authCode,
            "redirect_uri": self.callbackUrl
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {self.encodedCredentials}"
        }
        response = post(self.tokenUrl, data=data, headers=headers)
        if response.status_code == 200:
            data = response.json()
            self.get_tokens(data)
            self.write_token_data()
        else:
            print(f'Error reaching Schwab API, server response code: {response.status_code}')
            raise HTTPError

    def get_tokens(self, data) -> None:
        """
        Parses a .json object to return access token
        """
        self.tokens['Access']['Token'] = data['access_token']
        date_time = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
        self.tokens['Access']['Timestamp'] = date_time

        self.tokens['Refresh']['Token'] = data['refresh_token']
        self.tokens['Refresh']['Timestamp'] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")


    def use_refresh_token(self) -> None:
        """
        Uses refresh token to return new access token
        """
        if not self.encodedCredentials:
            self.get_environment_variables()
            self.encode_credentials()

        request_data = {
        "grant_type": "refresh_token",
        "refresh_token": self.tokens['Refresh']['Token'],
        "redirect_uri": self.callbackUrl
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {self.encodedCredentials}"
        }

        print(headers)

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
        """
        Wrapper function that handles checking for valid tokens and refreshing tokens if necessary
        """
        self.load_token_data()
        if self.check_for_valid_access_token():
            return True
        elif self.check_for_valid_refresh_token():
            self.use_refresh_token()
            self.write_token_data()
            return True
        else:
            print("Refresh token is expired")
            self.get_schwab_tokens()
            return True

if __name__ == '__main__':
    Schwab = SchwabAPICredentials()
    Schwab.token_handler()