import os
import time
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from notion_client import Client

# ========= 환경 변수 로드 =========
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# ========= 종목 및 이동평균 설정 =========
tickers = {
    "QLD": "QLD",
    "SSO": "SSO",
    "USD": "USD",
    "VIX": "^VIX"
}
ma_windows = [20, 60, 120, 200]

# ========= 이동평균 계산 함수 =========
def get_moving_averages(code, ma_windows):
    df = yf.download(code, period="1y")
    if df.empty:
        print(f"Warning: {code}의 데이터가 비어 있음")
        return None

    result = {
        "현재가": round(float(df["Close"].iloc[-1]), 2)
    }
    for window in ma_windows:
        result[f"MA{window}"] = round(df["Close"].rolling(window).mean().iloc[-1], 2)
    return result

# ========= 주봉 RSI 계산 함수 =========
def get_weekly_rsi(code):
    df = yf.download(code, period="1y", interval="1wk")
    if df.empty:
        print(f"Warning: {code}의 주봉 데이터가 비어 있음")
        return None
    delta = df["Close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2)

# ========= Fear & Greed Index 크롤링 =========
def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers)
    try:
        data = response.json()
        return int(data['fear_and_greed']['now']['score'])
    except Exception as e:
        print("Error: 공포지수 수집 실패:", e)
        return None

# ========= 노션 전송 =========
def send_to_notion(name, data):
    if data is None:
        print(f"[SKIP] 데이터 없음 → 전송 생략")
        return
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties={
                "지표명": {"title": [{"text": {"content": name}}]},
                "날짜": {"date": {"start": today}},
                **{
                    key: {"number": value}
                    for key, value in data.items()
                }
            }
        )
        print(f"[SUCCESS] {name} 저장 완료")
    except Exception as e:
        print(f"Error: Notion 저장 실패 ({name}):", e)

# ========= 실행 =========
for name, code in tickers.items():
    ma_data = get_moving_averages(code, ma_windows)
    time.sleep(5)  # 요청 제한 방지용 대기 시간
    rsi = get_weekly_rsi(code)
    if rsi is not None:
        ma_data["주봉RSI"] = rsi
    send_to_notion(name, ma_data)
    time.sleep(5)

fear_index = get_fear_and_greed()
send_to_notion("Fear & Greed", {"공포지수": fear_index})
