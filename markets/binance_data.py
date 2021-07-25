import os
from itertools import chain

import pandas as pd
from tqdm import tqdm

from binance import Client


class BinanceHistoricalData:
    TIME_INTERVALS = {
        "1m": "1min",
        "3m": "3min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "8h": "8H",
        "12h": "12H",
        "1d": "1D",
        "3d": "3D",
        "1w": "1W",
        "1M": "1M",
    }
    API_DATA_COLNAMES = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore",
    ]

    def __init__(self):
        self.client = Client(os.environ.get("BINANCE_KEY"), os.environ.get("BINANCE_SECRET"))

    def get_data(self, symbol, timeframe, start, end):
        if timeframe not in self.TIME_INTERVALS:
            raise ValueError(
                f"Incorrect timeframe: {timeframe}. Available options: {', '.join(self.TIME_INTERVALS)}"
            )

        daily_data = (
            bar for bar in
            tqdm(
                self.client.get_historical_klines_generator(symbol, timeframe, start, end, ),
                desc=f"Fetching daily data for: {symbol}, {timeframe}, {start} - {end}", total=len(pd.date_range(start, end, freq=self.TIME_INTERVALS[timeframe]))
            )
        )
        daily_data = pd.DataFrame(daily_data, columns=self.API_DATA_COLNAMES)
        daily_data["timestamp"] = pd.to_datetime(daily_data["timestamp"], unit="ms")

        return daily_data


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    binance_data = BinanceHistoricalData()
    bars = binance_data.get_data("BNBBUSD", "1h", "2021-07-10", "2021-07-19")
    print(bars)
