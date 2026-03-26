from auth.api_auth import SchwabAPICredentials
import sys



if __name__ == '__main__':
    Schwab = SchwabAPICredentials()
    Schwab.token_handler()
    access_token = Schwab.tokens['Access']['Token']
    headers = {"Authorization": f"Bearer {access_token}"}

    url = 'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=AAPL&periodType=year&period=1&frequencyType=daily&frequency=1'

    #response = requests.get(url, headers=headers)
    #print(response.text)
    print(sys.version)