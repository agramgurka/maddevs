# maddevs
Application for cryptocurrency exchange monitoring.

Fetches candles live data from the Binance and Bitfinex websocket APIs,
calculates RSI and VWAP on closed candles and displays result with set periodicity.
Initial data is fetched from the corresponding REST APIs.

##  Installation

```

git clone https://github.com/agramgurka/maddevs
python -m venv venv
pip install -r requirements.txt

```

## Usage

```

python run_client.py --periodicity integer (
    optional argument, time in seconds between data prints, default=5
)

```

## Additional info

Application was tested with python 3.11.1

