import asyncio
import json
import os
from datetime import datetime
import pytz
from websockets import connect
from termcolor import cprint

# list of symbols you wanna track
symbols = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'dogeusdt', 'wifusdt']
websocket_url_base = 'wss://fstream.binance.com/ws/'
trades_filename = 'binance_trades.csv'

# check if the csv file exists
if not os.path.isfile(trades_filename):
    with open(trades_filename, 'a') as f:
        f.write('Trade Time, Symbol, Aggregate Trade ID, Price, Quantity, USD Size, Is Buyer Maker\n')

class TradeAggregator:
    def __init__(self):
        self.trade_buckets = {}
    
    async def add_trade(self, symbol, second, usd_size, is_buyer_maker):
        trade_key = (symbol, second, is_buyer_maker)
        self.trade_buckets[trade_key] = self.trade_buckets.get(trade_key, 0) + usd_size

    async def check_and_print_trades(self):
        deletions = []
        for trade_key, usd_size in self.trade_buckets.items():
            symbol, second, is_buyer_maker = trade_key

            if usd_size >= 15000:
                trade_type = 'SELL' if is_buyer_maker else 'BUY'
                back_color = 'on_red' if trade_type == 'SELL' else 'on_green'
                stars = ''
                attrs = ['bold'] if usd_size >= 50000 else []
                repeat_count = 1
                if usd_size >= 500000:
                    stars = '*' * 2
                    repeat_count = 1
                    if trade_type == 'SELL':
                        back_color = 'on_magenta'
                    else:
                        back_color = 'on_blue'
                elif usd_size >= 100000:
                    stars = '*' * 1
                    repeat_count = 1
                
                output = f'{stars} {trade_type} {symbol} {second} ${usd_size:,.0f}'
                
                for _ in range(repeat_count):
                    cprint(output, 'white', f'{back_color}', attrs=attrs)
                        
            deletions.append(trade_key)

        for key in deletions:
            del self.trade_buckets[key]

trade_aggregator = TradeAggregator()

async def binance_trade_stream(uri, symbol, filename, aggregator):
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                agg_trade_id = data['a']
                price = float(data['p'])
                quantity = float(data['q'])
                is_buyer_maker = data['m']
                usd_size = price * quantity
                trade_time = datetime.fromtimestamp(int(data['T']) / 1000, pytz.timezone('Asia/Tokyo'))
                readable_trade_time = trade_time.strftime('%H:%M:%S')

                await aggregator.add_trade(symbol.upper().replace('USDT', ''), readable_trade_time, usd_size, is_buyer_maker)
                
                with open(filename, 'a') as f:
                    f.write(f'{trade_time}, {symbol.upper()}, {agg_trade_id}, {price}, {quantity}, {usd_size}, {is_buyer_maker}\n')

            except:
                await asyncio.sleep(5)

async def print_aggregated_trades_every_second(aggregator):
    while True:
        await asyncio.sleep(1)
        await aggregator.check_and_print_trades()

async def main():
    filename = 'C:/Users/Administrator/Documents/AlgoTrading/trading-data-streamer/binance_trades.csv'
    trade_stream_tasks = [binance_trade_stream(f'{websocket_url_base}{symbol}@aggTrade', symbol, filename, trade_aggregator) for symbol in symbols]
    print_tasks = asyncio.create_task(print_aggregated_trades_every_second(trade_aggregator))

    await asyncio.gather(*trade_stream_tasks, print_tasks)

asyncio.run(main())

