import os
import asyncio
import json
import aiohttp
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime, timezone
from telegram import Bot

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
VOLUME_MULTIPLIER = 50
COOLDOWN_FILE = "cooldowns.json"

bot = Bot(token=TELEGRAM_TOKEN)

# Загружаем, когда последний раз отправляли сигнал
def load_cooldowns():
    try:
        with open(COOLDOWN_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_cooldowns(data):
    with open(COOLDOWN_FILE, 'w') as f:
        json.dump(data, f)

last_signals = load_cooldowns()
now_ts = datetime.now().timestamp()

# Очищаем старые записи (старше 30 мин)
last_signals = {k: v for k, v in last_signals.items() if now_ts - v < 1800}

async def fetch_json(session, url, params=None):
    try:
        async with session.get(url, params=params, timeout=20) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        print(f"Ошибка {url}: {e}")
    return None

async def check_binance(session):
    # Берем только топ-50 по объему, чтобы уложиться в 5 минут
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    tickers = await fetch_json(session, url)
    if not tickers:
        return
    
    # Сортируем по объему (quoteVolume), берем топ-50
    top_pairs = sorted(tickers, key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)[:50]
    
    for ticker in top_pairs:
        symbol = ticker['symbol']
        if not symbol.endswith('USDT'):
            continue
            
        # Проверяем cooldown
        if last_signals.get(f"BN_{symbol}", 0) > now_ts - 1800:
            continue
            
        # Получаем свечи за последние 60 минут (1м)
        klines = await fetch_json(session, 
            f"https://fapi.binance.com/fapi/v1/klines",
            {"symbol": symbol, "interval": "1m", "limit": 60})
        
        if not klines or len(klines) < 10:
            continue
            
        volumes = [float(k[5]) for k in klines]
        avg_vol = sum(volumes[:-1]) / len(volumes[:-1])  # без последней свечи
        last_vol = volumes[-1]
        
        if avg_vol > 0 and last_vol >= avg_vol * VOLUME_MULTIPLIER:
            # Считаем изменение цены
            first_price = float(klines[0][1])
            last_price = float(klines[-1][4])
            change = ((last_price - first_price) / first_price) * 100
            
            # Рисуем график (упрощенно)
            fig, ax = plt.subplots(figsize=(10, 5))
            times = range(len(klines))
            prices = [float(k[4]) for k in klines]
            ax.plot(times, prices, color='green' if change >=0 else 'red')
            ax.set_title(f"{symbol} | Binance | {change:+.2f}%")
            buf = BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            plt.close()
            
            msg = (f"🚨 Сигнал: {symbol}\n"
                   f"Биржа: Binance\n"
                   f"Изменение 1ч: {change:+.2f}%\n"
                   f"Объем: {last_vol:,.0f} (средний: {avg_vol:,.0f})\n"
                   f"Превышение: ×{last_vol/avg_vol:.1f}")
            
            await bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=msg)
            last_signals[f"BN_{symbol}"] = now_ts
            print(f"✅ Отправлен сигнал {symbol}")

async def check_bybit(session):
    # Bybit - аналогично, топ-50
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    data = await fetch_json(session, url)
    if not data or data.get('retCode') != 0:
        return
        
    tickers = data['result']['list']
    top_pairs = sorted(tickers, key=lambda x: float(x.get('turnover24h', 0)), reverse=True)[:50]
    
    for ticker in top_pairs:
        symbol = ticker['symbol']
        if not symbol.endswith('USDT'):
            continue
            
        if last_signals.get(f"BB_{symbol}", 0) > now_ts - 1800:
            continue
            
        # Получаем свечи
        klines_data = await fetch_json(session,
            "https://api.bybit.com/v5/market/kline",
            {"category": "linear", "symbol": symbol, "interval": "1", "limit": "60"})
            
        if not klines_data or klines_data.get('retCode') != 0:
            continue
            
        klines = klines_data['result']['list']
        if len(klines) < 10:
            continue
            
        volumes = [float(k[5]) for k in klines]
        avg_vol = sum(volumes[:-1]) / len(volumes[:-1])
        last_vol = volumes[-1]
        
        if avg_vol > 0 and last_vol >= avg_vol * VOLUME_MULTIPLIER:
            first_price = float(klines[0][1])
            last_price = float(klines[-1][4])
            change = ((last_price - first_price) / first_price) * 100
            
            # График
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(range(len(klines)), [float(k[4]) for k in klines], 
                   color='green' if change >=0 else 'red')
            ax.set_title(f"{symbol} | Bybit | {change:+.2f}%")
            buf = BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            plt.close()
            
            msg = (f"🚨 Сигнал: {symbol}\n"
                   f"Биржа: Bybit\n"
                   f"Изменение 1ч: {change:+.2f}%\n"
                   f"Объем: {last_vol:,.0f} (средний: {avg_vol:,.0f})\n"
                   f"Превышение: ×{last_vol/avg_vol:.1f}")
            
            await bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=msg)
            last_signals[f"BB_{symbol}"] = now_ts

async def main():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            check_binance(session),
            check_bybit(session)
        )
    save_cooldowns(last_signals)
    print("✅ Проверка завершена")

if __name__ == "__main__":
    asyncio.run(main())
