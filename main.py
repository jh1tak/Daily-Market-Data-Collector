import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
from notion_client import Client

# ========== 환경 변수 ==========
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# ========== 기본 설정 ==========
tickers = {
    "QLD": "QLD",
    "SSO": "SSO",
    "USD": "DX-Y.NYB",
    "VIX": "^VIX"
}
ma_windows = [20, 60, 120, 200]

# ========== 보조 함수 ==========
def get_moving_averages(ticker, ma_list):
    df = yf.download(ticker, period="300d")["Close"]
    result = {
        "종목명": ticker,
        "현재가": round(df.iloc[-1], 2)
    }
    for ma in ma_list:
        result[f"MA{ma}"] = round(df.rolling(window=ma).mean().iloc[-1], 2)
    return result

def get_weekly_rsi(ticker, period=14):
    df = yf.download(ticker, period="1y", interval="1wk")["Close"]
    delta = df.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2)

def get_fear_and_greed():
    url = "https://edition.cnn.com/markets/fear-and-greed"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    try:
        val = soup.find("div", class_="FearGreedGraph__Dial-value").text.strip()
        return int(val)
    except Exception:
        return None

def get_session_tag():
    hour = datetime.now().hour
    return "오전" if hour < 12 else "오후"

# ========== Notion 전송 ==========
def send_to_notion(data: dict):
    properties = {
        "종목명": {"title": [{"text": {"content": data["종목명"]}}]},
        "날짜": {"date": {"start": datetime.now().isoformat()}},
        "세션": {"rich_text": [{"text": {"content": get_session_tag()}}]},
        "현재가": {"number": data.get("현재가")},
    }

    for ma in ma_windows:
        if f"MA{ma}" in data:
            properties[f"MA{ma}"] = {"number": data[f"MA{ma}"]}

    if "주봉RSI" in data:
        properties["주봉RSI"] = {"number": data["주봉RSI"]}
    if "공포지수" in data:
        properties["공포지수"] = {"number": data["공포지수"]}

    notion.pages.create(
        parent={"database_id": NOTION_DATABASE_ID},
        properties=properties
    )

# ========== 실행 ==========
for name, code in tickers.items():
    if name == "VIX":
        rsi = get_weekly_rsi(code)
        vix_price = round(yf.download(code, period="5d")["Close"].iloc[-1], 2)
        send_to_notion({
            "종목명": "VIX",
            "현재가": vix_price,
            "주봉RSI": rsi
        })
    else:
        ma_data = get_moving_averages(code, ma_windows)
        send_to_notion(ma_data)

fg_index = get_fear_and_greed()
if fg_index is not None:
    send_to_notion({
        "종목명": "Fear & Greed",
        "공포지수": fg_index
    })
