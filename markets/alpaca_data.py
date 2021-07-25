import pandas as pd
from tqdm import tqdm

from alpaca_trade_api.rest import REST, TimeFrame


class AlpacaHistoricalData:
    def __init__(self):
        self.api = REST()
        self.timeframes = {tf.value: tf for tf in TimeFrame}

    def get_data(self, symbol, timeframe, start, end):
        if timeframe not in self.timeframes.keys():
            raise ValueError(
                f"Incorrect timeframe: {timeframe}. Available options: {', '.join(self.timeframes.keys())}"
            )

        date_range = [dt.date() for dt in pd.date_range(start, end, freq="D")]
        daily_data = (
            self.api.get_bars(
                symbol, self.timeframes[timeframe], date, date, limit=10000, adjustment="raw"
            ).df
            for date in tqdm(date_range, desc=f"Fetching data for: {symbol}, {timeframe}, {start} - {end}")
        )

        return pd.concat(daily_data)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    alpaca_data = AlpacaHistoricalData()
    bars = alpaca_data.get_data("AAPL", "1Hour", "2021-07-01", "2021-07-19")
    print(bars)
