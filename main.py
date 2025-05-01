import os
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from notion_client import Client

# ========= 환경 변수 =========
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
notion = Client(auth=NOTION_TOKEN)

# ========= 기본 설정 =========
tickers = {
    "QLD": "QLD",
    "SSO": "SSO",
    "USD": "DX-Y.NYB",
    "VIX": "^VIX"
}
ma_windows = [20, 60, 120, 200]

# ========= 이동평균 수집 =========
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
        "현재가": round(float(df.iloc[-1]), 2)
    }
    for ma in ma_list:
        try:
            ma_value = df.rolling(window=ma).mean().iloc[-1]
            if isinstance(ma_value, pd.Series):
                ma_value = ma_value.iloc[0]
            result[f"MA{ma}"] = round(float(ma_value), 2)
        except Exception as e:
            print(f"[WARNING] {ticker} MA{ma} 계산 실패: {e}")

    return result

# ========= 주봉 RSI =========
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
    return float(round(rsi.iloc[-1], 2))

# ========= CNN JSON API 공포지수 =========
def get_fear_and_greed():
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        score = data["fear_and_greed"]["score"]
        return int(score)
    except Exception as e:
        print(f"[ERROR] 공포지수 수집 실패: {e}")
        return None

# ========= 세션 태그 =========
def get_session_tag():
    hour = datetime.now().hour
    return "오전" if hour < 12 else "오후"

# ========= Notion 전송 =========
def send_to_notion(data: dict):
    if data is None:
        print(f"[SKIP] 데이터 없음 → 전송 생략")
        return

    properties = {
        "지표명": {"title": [{"text": {"content": data["종목명"]}}]},
        "날짜": {"date": {"start": datetime.now().isoformat()}},
        "세션": {"rich_text": [{"text": {"content": get_session_tag()}}]}
    }

    for key, value in data.items():
        if key in ["현재가", "공포지수", "주봉RSI"] or key.startswith("MA"):
            properties[key] = {"number": value}

    try:
        notion.pages.create(parent={"database_id": NOTION_DATABASE_ID}, properties=properties)
        print(f"[SUCCESS] {data['종목명']} 저장 완료")
    except Exception as e:
        print(f"[ERROR] Notion 저장 실패 ({data['종목명']}): {e}")

# ========= 실행 =========
for name, code in tickers.items():
    if name == "VIX":
        rsi = get_weekly_rsi(code)
        try:
            price_df = yf.download(code, period="5d", auto_adjust=False)["Close"]
            price = round(float(price_df.iloc[-1]), 2) if not price_df.empty else None
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

fg_index = get_fear_and_greed()
if fg_index is not None:
    send_to_notion({
        "종목명": "Fear & Greed",
        "공포지수": fg_index
    })
