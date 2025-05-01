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
    try:
        df = yf.download(ticker, period="300d", auto_adjust=False)["Close"]
    except Exception as e:
        print(f"[ERROR] {ticker} 다운로드 실패: {e}")
        return None

    if df.empty:
        print(f"[WARNING] {ticker}의 데이터가 비어 있음")
        return None

    result = {
        "종목명": ticker,
        "현재가": round(df.iloc[-1], 2)
    }
    for ma in ma_list:
        result[f"MA{ma}"] = round(df.rolling(window=ma).mean().iloc[-1], 2)

    return result

def get_weekly_rsi(ticker, period=14):
    try:
        df = yf.download(ticker, period="1y", interval="1wk", auto_adjust=False)["Close"]
    except Exception as e:
        print(f"[ERROR] {ticker} RSI 다운로드 실패: {e}")
        return None

    if df.empty:
        print(f"[WARNING] {ticker}의 주봉 데이터가 비어 있음")
        return None

    delta = df.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2)

def get_fear_and_greed():
    try:
        url = "https://edition.cnn.com/markets/fear-and-greed"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        val = soup.find("div", class_="FearGreedGraph__Dial-value").text.strip()
        return int(val)
    except Exception as e:
        print(f"[ERROR] 공포지수 수집 실패: {e}")
        return None

def get_session_tag():
    hour = datetime.now().hour
    return "오전" if hour < 12 else "오후"

# ========== Notion 전송 ==========
def send_to_notion(data: dict):
    if data is None:
        return

    properties = {
        "지표명": {"title": [{"text": {"content": data["종목명"]}}]},
        "날짜": {"date": {"start": datetime.now().isoformat()}},
        "세션": {"rich_text": [{"text": {"content": get_session_tag()}}]},
    }

    if "현재가" in data:
        properties["현재가"] = {"number": data["현재가"]}
    for ma in ma_windows:
        key = f"MA{ma}"
        if key in data:
            properties[key] = {"number": data[key]}
    if "주봉RSI" in data:
        properties["주봉RSI"] = {"number": data["주봉RSI"]}
    if "공포지수" in data:
        properties["공포지수"] = {"number": data["공포지수"]}

    try:
        notion.pages.create(parent={"database_id": NOTION_DATABASE_ID}, properties=properties)
        print(f"[INFO] {data['종목명']} 저장 성공")
    except Exception as e:
        print(f"[ERROR] Notion 저장 실패 ({data['종목명']}): {e}")

# ========== 실행 ==========
for name, code in tickers.items():
    if name == "VIX":
        rsi = get_weekly_rsi(code)
        try:
            price_df = yf.download(code, period="5d", auto_adjust=False)["Close"]
            price = round(price_df.iloc[-1], 2) if not price_df.empty else None
        except Exception:
            price = None

        send_to_notion({
            "종목명": "VIX",
            "현재가": price,
            "주봉RSI": rsi
        })
    else:
        ma_data = get_moving_averages(code, ma_windows)
        send_to_notion(ma_data)

# 공포지수
fg_index = get_fear_and_greed()
if fg_index is not None:
    send_to_notion({
        "종목명": "Fear & Greed",
        "공포지수": fg_index
    })
