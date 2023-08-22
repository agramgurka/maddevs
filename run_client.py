import argparse
import asyncio
import logging
import sys
from dataclasses import asdict, dataclass

import loguru
import pandas as pd
import pandas_ta as pta
from aio_binance.futures.usdt import Client as APIClient
from aio_binance.futures.usdt import WsClient
from bfxapi import PUB_REST_HOST, PUB_WSS_HOST
from bfxapi import Client as BFXClient
from bfxapi.websocket.enums import Channel

logger = logging.getLogger(__name__)


@dataclass
class LastDataContainer:
    binance: dict | None = None
    bitfinex: dict | None = None


@dataclass
class BinanceUpdate:
    closed_price: str | None = None
    rsi: float | None = None


@dataclass
class BitfinexUpdate:
    closed_price: str | None = None
    vwap: float | None = None


class BaseSubscriber:
    def __init__(self, on_update, symbol, interval):
        self.on_update = on_update
        self.symbol = symbol
        self.interval = interval
        self.data = []

    def calculate_values(self):
        raise NotImplementedError()

    async def start(self):
        raise NotImplementedError()


class BinanceSubscriber(BaseSubscriber):
    def __init__(self, on_update, symbol, interval):
        loguru.logger.remove()
        loguru.logger.add(sys.stderr, level="WARNING")
        super().__init__(on_update, symbol, interval)
        self.client = WsClient()

    def calculate_values(self):
        rsi = pta.rsi(pd.Series(data=self.data), length=14).iat[14]
        return rsi

    async def init_values(self):
        api_client = APIClient()
        historic_values = await api_client.get_public_klines(self.symbol, self.interval, limit=14 + 1)
        self.data = pd.DataFrame(historic_values['data'])[4].astype(float).tolist()

    async def start(self):
        await self.init_values()
        self.on_update(BinanceUpdate(self.data[-1], self.calculate_values()))

        async def adapter_event(data):
            if data["k"]["x"]:
                self.data.pop(0)
                self.data.append(float(data["k"]["c"]))
                self.on_update(BinanceUpdate(self.data[-1], self.calculate_values()))

        return await self.client.stream_kline(self.symbol, self.interval, adapter_event)


class BitfinexSubscriber(BaseSubscriber):
    def __init__(self, on_update, symbol, interval):
        super().__init__(on_update, symbol, interval)
        self.client = BFXClient(wss_host=PUB_WSS_HOST)
        self.prev_closed = None

    def calculate_values(self):
        data = pd.DataFrame(self.data)
        data['dt'] = pd.to_datetime(data['mts'], unit='ms')
        data = data.set_index('dt')
        vwap = pta.vwap(data['high'], data['low'], data['close'], data['volume']).iat[-1]
        return vwap

    async def start(self):
        api_client = BFXClient(rest_host=PUB_REST_HOST)
        self.data.extend(api_client.rest.public.get_candles_hist(self.symbol, self.interval, limit=14))
        self.data.sort(key=lambda el: el.mts)
        self.on_update(BitfinexUpdate(self.data[-1].close, self.calculate_values()))

        def on_candles_update(sub, candle):
            logger.debug(f"Candle update for key <{sub['key']}>: {candle}")
            if not self.prev_closed:
                self.prev_closed = candle
                if not self.data[-1] == self.prev_closed:
                    self.data.pop(0)
                    self.data.append(self.prev_closed)
                    self.on_update(BitfinexUpdate(self.data[-1].close, self.calculate_values()))
            else:
                self.prev_closed = None

        async def on_open():
            await self.client.wss.subscribe(Channel.CANDLES, key=f'trade:{self.interval}:{self.symbol}')

        self.client.wss.on("candles_update", callback=on_candles_update)
        self.client.wss.on("open", callback=on_open)

        return await self.client.wss.start()


class Client:
    def __init__(self,
                 print_state_period,
                 binance_symbol="BTCUSDT",
                 binance_interval="5m",
                 bitfinex_symbol="tBTCUSD",
                 bitfinex_interval="1m"):
        self.last_data = LastDataContainer()
        self.print_state_period = print_state_period

        def update_binance(data):
            self.last_data.binance = asdict(data)

        def update_bitfinex(data):
            self.last_data.bitfinex = asdict(data)

        self.binance = BinanceSubscriber(update_binance, binance_symbol, binance_interval)
        self.bitfinex = BitfinexSubscriber(update_bitfinex, bitfinex_symbol, bitfinex_interval)
        self.tasks = []

    async def print_state(self):
        while True:
            print(asdict(self.last_data))
            await asyncio.sleep(self.print_state_period)

    async def start(self):
        self.tasks.append(asyncio.create_task(self.binance.start()))
        self.tasks.append(asyncio.create_task(self.bitfinex.start()))
        self.tasks.append(asyncio.create_task(self.print_state()))

    async def join(self):
        await asyncio.gather(*self.tasks)


async def main(print_state_period=5):
    client = Client(print_state_period)
    await client.start()
    await client.join()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(prog='maddevstest')
    parser.add_argument('--periodicity', default=5, help='set periodicity of displaying values', type=int)
    args = parser.parse_args()
    asyncio.run(main(args.periodicity))
