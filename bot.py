import os
import asyncio
import logging
import time
import random
from datetime import datetime, timezone
from io import BytesIO
from collections import defaultdict

import aiohttp
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from telegram import Bot
from telegram.constants import ParseMode

# ─── НАСТРОЙКИ ───────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
VOLUME_MULTIPLIER = 50
CHECK_INTERVAL_SECONDS = 60
COOLDOWN_MINUTES = 30

# ─── БЕСПЛАТНЫЕ ПРОКСИ (автоматическая ротация) ─────────────
PROXY_LIST = [
    "http://45.95.84.26:8080",
    "http://103.152.112.184:8080",
    "http://103.224.212.226:3128",
    "http://103.28.54.96:3128",
    "http://115.211.6.89:8080",
    "http://116.202.93.194:8080",
    "http://124.223.47.213:8080",
    "http://139.99.105.146:8888",
    "http://150.109.28.178:3128",
    "http://159.89.211.12:8080",
    "http://167.71.193.186:8080",
    "http://167.99.191.204:8080",
    "http://178.128.61.235:8080",
    "http://185.220.101.34:8080",
    "http://194.165.16.80:8080",
    "http://195.201.49.197:8080",
    "http://20.118.160.104:8080",
    "http://206.189.126.10:8080",
    "http://45.77.185.160:8080",
    "http://45.83.208.168:8080",
    "http://51.15.18.168:8080",
    "http://54.37.18.76:8080",
    "http://64.227.77.222:8080",
    "http://66.102.6.196:8080",
    "http://68.183.49.14:8080",
]

async def get_random_proxy():
    """Возвращает случайный рабочий прокси."""
    proxy = random.choice(PROXY_LIST)
    return {"http": proxy, "https": proxy}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_TOKEN)
last_signal_time: dict[str, float] = defaultdict(float)


# ─── ПОЛУЧЕНИЕ ДАННЫХ С БИРЖ ────────────────────────────────

async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict = None, use_proxy=True) -> dict | list | None:
    """Безопасный GET-запрос через прокси."""
    proxy = None
    if use_proxy:
        try:
            proxy = await get_random_proxy()
        except:
            pass
    
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30), proxy=proxy) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                logger.warning(f"HTTP {resp.status} для {url}")
                return None
    except Exception as e:
        logger.error(f"Ошибка запроса {url}: {e}")
        return None


# ──────── BINANCE ────────

async def binance_get_futures_symbols(session: aiohttp.ClientSession) -> list[str]:
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    data = await fetch_json(session, url)
    if not data:
        return []
    symbols = []
    for s in data.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            symbols.append(s["symbol"])
    return symbols


async def binance_get_klines_1m(session: aiohttp.ClientSession, symbol: str, limit: int = 60) -> list | None:
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": "1m", "limit": limit}
    return await fetch_json(session, url, params)


async def binance_get_24h_volume(session: aiohttp.ClientSession, symbol: str) -> dict | None:
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    params = {"symbol": symbol}
    return await fetch_json(session, url, params)


async def binance_get_klines_1m_24h(session: aiohttp.ClientSession, symbol: str) -> list | None:
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": "1m", "limit": 1440}
    return await fetch_json(session, url, params)


# ──────── BYBIT ────────

async def bybit_get_futures_symbols(session: aiohttp.ClientSession) -> list[str]:
    url = "https://api.bybit.com/v5/market/instruments-info"
    params = {"category": "linear", "limit": "1000"}
    data = await fetch_json(session, url, params)
    if not data or data.get("retCode") != 0:
        return []
    symbols = []
    for item in data.get("result", {}).get("list", []):
        if item.get("quoteCoin") == "USDT" and item.get("status") == "Trading":
            symbols.append(item["symbol"])
    return symbols


async def bybit_get_klines_1m(session: aiohttp.ClientSession, symbol: str, limit: int = 60) -> list | None:
    url = "https://api.bybit.com/v5/market/kline"
    params = {"category": "linear", "symbol": symbol, "interval": "1", "limit": str(limit)}
    data = await fetch_json(session, url, params)
    if not data or data.get("retCode") != 0:
        return None
    return data.get("result", {}).get("list", [])


async def bybit_get_klines_1m_24h(session: aiohttp.ClientSession, symbol: str) -> list | None:
    url = "https://api.bybit.com/v5/market/kline"
    all_klines = []

    params1 = {"category": "linear", "symbol": symbol, "interval": "1", "limit": "1000"}
    data1 = await fetch_json(session, url, params1)
    if not data1 or data1.get("retCode") != 0:
        return None
    list1 = data1.get("result", {}).get("list", [])
    all_klines.extend(list1)

    if len(list1) == 1000:
        oldest_ts = int(list1[-1][0])
        params2 = {"category": "linear", "symbol": symbol, "interval": "1", "limit": "440", "end": str(oldest_ts)}
        data2 = await fetch_json(session, url, params2)
        if data2 and data2.get("retCode") == 0:
            list2 = data2.get("result", {}).get("list", [])
            all_klines.extend(list2)

    return all_klines


# ─── АНАЛИЗ ──────────────────────────────────────────────────

def parse_binance_klines(raw_klines: list) -> pd.DataFrame:
    df = pd.DataFrame(raw_klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close"] = df["close"].astype(float)
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["quote_volume"] = df["quote_volume"].astype(float)
    return df


def parse_bybit_klines(raw_klines: list) -> pd.DataFrame:
    df = pd.DataFrame(raw_klines, columns=[
        "open_time", "open", "high", "low", "close", "volume", "turnover"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"].astype(int), unit="ms", utc=True)
    df["close"] = df["close"].astype(float)
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["turnover"] = df["turnover"].astype(float)
    df = df.sort_values("open_time").reset_index(drop=True)
    return df


def compute_avg_volume_24h(df_24h: pd.DataFrame) -> float:
    if df_24h.empty:
        return 0.0
    return df_24h["volume"].mean()


def compute_price_change_1h(df_1h: pd.DataFrame) -> float:
    if len(df_1h) < 2:
        return 0.0
    first_close = df_1h.iloc[0]["open"]
    last_close = df_1h.iloc[-1]["close"]
    if first_close == 0:
        return 0.0
    return ((last_close - first_close) / first_close) * 100


# ─── ГРАФИК ─────────────────────────────────────────────────

def create_chart(df_1h: pd.DataFrame, symbol: str, exchange: str, price_change: float) -> BytesIO:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), height_ratios=[3, 1],
                                     gridspec_kw={'hspace': 0.3})
    fig.patch.set_facecolor('#1a1a2e')

    times = df_1h["open_time"]
    closes = df_1h["close"]
    volumes = df_1h["volume"]

    color = '#00ff88' if price_change >= 0 else '#ff4444'

    ax1.set_facecolor('#16213e')
    ax1.plot(times, closes, color=color, linewidth=2, label='Цена')
    ax1.fill_between(times, closes.min() * 0.999, closes, alpha=0.15, color=color)

    ax1.set_title(f"📊 {symbol} ({exchange}) | 1-мин свечи за 1 час", 
                  fontsize=14, fontweight='bold', color='white', pad=10)
    ax1.set_ylabel("Цена (USDT)", fontsize=11, color='white')
    ax1.tick_params(colors='white', labelsize=9)
    ax1.grid(True, alpha=0.2, color='gray')
    ax1.spines['bottom'].set_color('gray')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_color('gray')

    last_price = closes.iloc[-1]
    ax1.annotate(f'{last_price:.4f}', xy=(times.iloc[-1], last_price),
                 fontsize=10, color=color, fontweight='bold',
                 xytext=(10, 10), textcoords='offset points',
                 arrowprops=dict(arrowstyle='->', color=color, lw=1.5))

    ax2.set_facecolor('#16213e')
    bar_colors = []
    for i in range(len(df_1h)):
        if df_1h.iloc[i]["close"] >= df_1h.iloc[i]["open"]:
            bar_colors.append('#00ff88')
        else:
            bar_colors.append('#ff4444')
    
    ax2.bar(times, volumes, color=bar_colors, alpha=0.7, width=0.0006)
    ax2.set_ylabel("Объём", fontsize=11, color='white')
    ax2.set_xlabel("Время (UTC)", fontsize=11, color='white')
    ax2.tick_params(colors='white', labelsize=9)
    ax2.grid(True, alpha=0.2, color='gray')
    ax2.spines['bottom'].set_color('gray')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['left'].set_color('gray')

    ax2.bar(times.iloc[-1], volumes.iloc[-1], color='#ffff00', alpha=0.9, width=0.0006)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    plt.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=120, bbox_inches='tight',
                facecolor=fig.get_facecolor(), edgecolor='none')
    buf.seek(0)
    plt.close(fig)
    return buf


# ─── СООБЩЕНИЕ ───────────────────────────────────────────────

def format_signal_message(
    symbol: str,
    exchange: str,
    price_change: float,
    avg_volume_24h: float,
    last_minute_volume: float,
    volume_ratio: float,
    last_price: float
) -> str:
    if price_change > 0:
        direction = "🟢 РОСТ"
        arrow = "📈"
    elif price_change < 0:
        direction = "🔴 ПАДЕНИЕ"
        arrow = "📉"
    else:
        direction = "⚪ БЕЗ ИЗМЕНЕНИЙ"
        arrow = "➡️"

    coin_name = symbol.replace("USDT", "")

    msg = (
        f"🚨 <b>СИГНАЛ АНОМАЛЬНОГО ОБЪЁМА</b> 🚨\n"
        f"{'━' * 35}\n\n"
        f"🪙 <b>Монета:</b> {coin_name} ({symbol})\n"
        f"🏦 <b>Биржа:</b> {exchange}\n"
        f"💰 <b>Текущая цена:</b> {last_price:.6g} USDT\n\n"
        f"{arrow} <b>Изменение цены (1ч):</b> {price_change:+.2f}%  {direction}\n\n"
        f"📊 <b>Объёмы:</b>\n"
        f"   • Средний объём/мин (24ч): {avg_volume_24h:,.2f}\n"
        f"   • Объём за последнюю минуту: {last_minute_volume:,.2f}\n"
        f"   • <b>Превышение: ×{volume_ratio:.1f}</b> 🔥\n\n"
        f"⏰ <b>Время сигнала:</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"{'━' * 35}"
    )
    return msg


# ─── ОТПРАВКА ────────────────────────────────────────────────

async def send_signal(
    symbol: str,
    exchange: str,
    price_change: float,
    avg_volume_24h: float,
    last_minute_volume: float,
    volume_ratio: float,
    last_price: float,
    df_1h: pd.DataFrame
):
    key = f"{exchange}:{symbol}"
    now = time.time()
    if now - last_signal_time[key] < COOLDOWN_MINUTES * 60:
        logger.info(f"Кулдаун для {key}, пропускаем")
        return
    last_signal_time[key] = now

    message = format_signal_message(
        symbol, exchange, price_change,
        avg_volume_24h, last_minute_volume, volume_ratio, last_price
    )

    chart_buf = create_chart(df_1h, symbol, exchange, price_change)

    try:
        await bot.send_photo(
            chat_id=CHAT_ID,
            photo=chart_buf,
            caption=message,
            parse_mode=ParseMode.HTML
        )
        logger.info(f"✅ Сигнал отправлен: {symbol} ({exchange}) ×{volume_ratio:.1f}")
    except Exception as e:
        logger.error(f"Ошибка отправки сигнала: {e}")


# ─── ПРОВЕРКА СИМВОЛОВ ───────────────────────────────────────

async def check_symbol_binance(session: aiohttp.ClientSession, symbol: str):
    try:
        klines_1h = await binance_get_klines_1m(session, symbol, limit=60)
        if not klines_1h or len(klines_1h) < 5:
            return

        df_1h = parse_binance_klines(klines_1h)
        last_minute_volume = df_1h.iloc[-1]["volume"]

        if last_minute_volume == 0:
            return

        klines_24h = await binance_get_klines_1m_24h(session, symbol)
        if not klines_24h or len(klines_24h) < 100:
            return

        df_24h = parse_binance_klines(klines_24h)
        avg_vol = compute_avg_volume_24h(df_24h.iloc[:-1])

        if avg_vol == 0:
            return

        volume_ratio = last_minute_volume / avg_vol

        if volume_ratio >= VOLUME_MULTIPLIER:
            price_change = compute_price_change_1h(df_1h)
            last_price = df_1h.iloc[-1]["close"]

            logger.info(f"🔥 BINANCE {symbol}: объём ×{volume_ratio:.1f} | цена {price_change:+.2f}%")

            await send_signal(
                symbol=symbol,
                exchange="Binance Futures",
                price_change=price_change,
                avg_volume_24h=avg_vol,
                last_minute_volume=last_minute_volume,
                volume_ratio=volume_ratio,
                last_price=last_price,
                df_1h=df_1h
            )
    except Exception as e:
        logger.error(f"Ошибка проверки Binance {symbol}: {e}")


async def check_symbol_bybit(session: aiohttp.ClientSession, symbol: str):
    try:
        klines_1h_raw = await bybit_get_klines_1m(session, symbol, limit=60)
        if not klines_1h_raw or len(klines_1h_raw) < 5:
            return

        df_1h = parse_bybit_klines(klines_1h_raw)
        last_minute_volume = df_1h.iloc[-1]["volume"]

        if last_minute_volume == 0:
            return

        klines_24h_raw = await bybit_get_klines_1m_24h(session, symbol)
        if not klines_24h_raw or len(klines_24h_raw) < 100:
            return

        df_24h = parse_bybit_klines(klines_24h_raw)
        avg_vol = compute_avg_volume_24h(df_24h.iloc[:-1])

        if avg_vol == 0:
            return

        volume_ratio = last_minute_volume / avg_vol

        if volume_ratio >= VOLUME_MULTIPLIER:
            price_change = compute_price_change_1h(df_1h)
            last_price = df_1h.iloc[-1]["close"]

            logger.info(f"🔥 BYBIT {symbol}: объём ×{volume_ratio:.1f} | цена {price_change:+.2f}%")

            await send_signal(
                symbol=symbol,
                exchange="Bybit Futures",
                price_change=price_change,
                avg_volume_24h=avg_vol,
                last_minute_volume=last_minute_volume,
                volume_ratio=volume_ratio,
                last_price=last_price,
                df_1h=df_1h
            )
    except Exception as e:
        logger.error(f"Ошибка проверки Bybit {symbol}: {e}")


# ─── ГЛАВНЫЙ ЦИКЛ ────────────────────────────────────────────

async def run_check():
    logger.info("=" * 50)
    logger.info("🔍 Начинаем цикл проверки...")

    async with aiohttp.ClientSession() as session:
        binance_symbols, bybit_symbols = await asyncio.gather(
            binance_get_futures_symbols(session),
            bybit_get_futures_symbols(session)
        )

        logger.info(f"Binance: {len(binance_symbols)} пар | Bybit: {len(bybit_symbols)} пар")

        batch_size = 10
        delay_between_batches = 2

        logger.info("📡 Проверяем Binance...")
        for i in range(0, len(binance_symbols), batch_size):
            batch = binance_symbols[i:i + batch_size]
            tasks = [check_symbol_binance(session, sym) for sym in batch]
            await asyncio.gather(*tasks)
            await asyncio.sleep(delay_between_batches)

        logger.info("📡 Проверяем Bybit...")
        for i in range(0, len(bybit_symbols), batch_size):
            batch = bybit_symbols[i:i + batch_size]
            tasks = [check_symbol_bybit(session, sym) for sym in batch]
            await asyncio.gather(*tasks)
            await asyncio.sleep(delay_between_batches)

    logger.info("✅ Цикл проверки завершён")


# ─── ВЕБ-СЕРВЕР ──────────────────────────────────────────────

from aiohttp import web

async def health_handler(request):
    return web.Response(text="OK", status=200)


async def run_web_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)

    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"🌐 Веб-сервер запущен на порту {port}")


# ─── ЗАПУСК ──────────────────────────────────────────────────

async def main():
    logger.info("🤖 Бот запускается...")
    logger.info(f"📋 Порог объёма: ×{VOLUME_MULTIPLIER}, интервал: {CHECK_INTERVAL_SECONDS}с")

    await run_web_server()

    try:
        start_msg = (
            f"🤖 <b>Бот сигналов запущен!</b>\n\n"
            f"📋 <b>Настройки:</b>\n"
            f"   • Биржи: Binance + Bybit (фьючерсы)\n"
            f"   • Порог объёма: ×{VOLUME_MULTIPLIER}\n"
            f"   • Интервал проверки: {CHECK_INTERVAL_SECONDS} сек\n\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
        await bot.send_message(chat_id=CHAT_ID, text=start_msg, parse_mode=ParseMode.HTML)
        logger.info("✅ Стартовое сообщение отправлено")
    except Exception as e:
        logger.error(f"❌ Ошибка стартового сообщения: {e}")
        return

    while True:
        try:
            await run_check()
        except Exception as e:
            logger.error(f"❌ Ошибка цикла: {e}")

        logger.info(f"💤 Ожидание {CHECK_INTERVAL_SECONDS} секунд...")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
