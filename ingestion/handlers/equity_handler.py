from auth import api_auth
from requests import get

if __name__ == '__main__':
    Schwab = api_auth.SchwabAPICredentials()
    Schwab.token_handler()
    access_token = Schwab.tokens['Access']['Token']
    headers = {"Authorization": f"Bearer {access_token}"}

    url = 'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=AAPL&periodType=year&period=1&frequencyType=daily&frequency=1'

    response = get(url, headers=headers)
    print(response.text)