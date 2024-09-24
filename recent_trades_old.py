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
trades_filename = 'D:/Work/Trading/data-stream/binance_trades.csv'

# check if the csv file exists
if not os.path.isfile(trades_filename):
    with open(trades_filename, 'w') as f:
        f.write('Event Time, Symbol, Aggregate Trade ID, Price, Quantity, Trade Time, Is Buyer Maker\n')

async def binance_trade_stream(uri, symbol, filename):
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                event_time = int(data['E'])
                agg_trade_id = data['a']
                price = float(data['p'])
                quantity = float(data['q'])
                trade_time = int(data['T'])
                is_buyer_maker = data['m']
                tz = pytz.timezone('Asia/Tokyo')
                readable_trade_time = datetime.fromtimestamp(trade_time / 1000, tz).strftime('%H:%M:%S')
                usd_size = price * quantity
                display_symbol = symbol.upper().replace('USDT', '')

                if usd_size >= 15000:
                    trade_type = 'SELL' if is_buyer_maker else 'BUY'
                    color = 'red' if trade_type == 'SELL' else 'green'
                    stars = ''
                    attrs = ['bold'] if usd_size >= 50000 else []
                    repeat_count = 1
                    if usd_size >= 500000:
                        stars = '*' * 2
                        repeat_count = 1
                        if trade_type == 'SELL':
                            color = 'magenta'
                        else:
                            color = 'blue'
                    elif usd_size >= 100000:
                        stars = '*' * 1
                        repeat_count = 1
                    
                    output = f"{stars} {trade_type} {display_symbol} {readable_trade_time} ${usd_size:,.0f} "
                    
                    for _ in range(repeat_count):
                        cprint(output, 'white', f'on_{color}', attrs=attrs)
                    
                    # log to csv
                    with open(filename, 'a') as f:
                        f.write(f'{event_time}, {symbol.upper()}, {agg_trade_id}, {price}, {quantity}, {trade_time}, {is_buyer_maker}\n')
            
            except:
                await asyncio.sleep(5)

async def main():
    filename = 'D:/Work/Trading/data-stream/binance_trades.csv'

    # create a task for each symbol  trade stream
    tasks = []
    for symbol in symbols:
        stream_url = f'{websocket_url_base}{symbol}@aggTrade'
        tasks.append(binance_trade_stream(stream_url, symbol, filename))

    await asyncio.gather(*tasks)

asyncio.run(main())