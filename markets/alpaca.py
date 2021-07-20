import os
import logging
import json
import requests
import pandas as pd


class AlpacaHistoricalData:
    def __init__(self):
        self.headers = {
            "APCA-API-KEY-ID": os.environ["APCA_API_KEY_ID"],
            "APCA-API-SECRET-KEY": os.environ["APCA_API_SECRET_KEY"],
        }
        self.APCA_API_DATA_URL = os.environ["APCA_API_DATA_URL"]

    def get_symbol_data(self, symbol, start, end, timeframe="1Min"):
        symbol_bars_url = f"{self.APCA_API_DATA_URL}/v2/stocks/{symbol}/bars"
        page_token = None
        symbol_bars = []

        while (page_token is not None) ^ (len(symbol_bars) == 0):
            fetched_data = self.fetch_data(
                symbol_bars_url, start, end, limit=10000, timeframe=timeframe, page_token=page_token
            )
            bars = fetched_data.get("bars")
            if bars:
                symbol_bars.extend(bars)
                page_token = fetched_data.get("next_page_token")
            else:
                break

        symbol_bars = pd.DataFrame(data=symbol_bars)
        symbol_bars.rename(columns=["timestamp", "open", "high", "low", "close", "volume"])

        return symbol_bars

    def fetch_data(self, bars_url, start, end, limit, timeframe, page_token):
        assert timeframe in ("1Min", "1Hour", "1Day"), "Available values are: 1Min, 1Hour, 1Day."
        assert start <= end
        assert 0 < limit <= 10000

        data = {
            "start": start,
            "end": end,
            "limit": limit,
            "page_token": page_token,
            "timeframe": timeframe,
            "adjustment": 'adjusted'
        }
        data = {k: v for k, v in data.items() if v is not None}
        response = requests.get(bars_url, headers=self.headers, params=data)
        if not response.ok:
            logging.warning(f"{response} {response.reason} {response.content}")
        return json.loads(response.content)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    alpaca_data = AlpacaHistoricalData()
    bars = alpaca_data.get_symbol_data("AAPL", "2021-02-08", "2021-02-08", "1Hour")
    print(bars)
