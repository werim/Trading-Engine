import pandas as pd
import requests
import os
from datetime import datetime


def fetch_binance_klines(symbol, interval, start_time):
    """Binance Futures API'den geçmiş mum verilerini çeker."""
    url = "https://fapi.binance.com/fapi/v1/klines"
    # Zamanı milisaniyeye çevir
    start_ts = int(pd.to_datetime(start_time).timestamp() * 1000)

    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_ts,
        'limit': 500
    }

    try:
        resp = requests.get(url, params=params)
        data = resp.json()
        df = pd.DataFrame(data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'q_volume', 'trades', 'taker_base', 'taker_quote', 'ignore'
        ])
        # Sayısal sütunları dönüştür
        for col in ['open', 'high', 'low', 'close']:
            df[col] = df[col].astype(float)
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        return df
    except Exception as e:
        print(f"Hata ({symbol}): {e}")
        return None


def analyze_orders(csv_path):
    df = pd.read_csv(csv_path)
    results = []

    print(f"--- Analiz Başlatıldı: {len(df)} Emir İnceleniyor ---")

    for i, row in df.iterrows():
        symbol = row['symbol']
        side = row['side']
        entry = float(row['entry_trigger'])
        tp = float(row['tp'])
        sl = float(row['sl'])
        created_at = row['created_at']

        # Fiyat verisini çek
        klines = fetch_binance_klines(symbol, '1h', created_at)

        if klines is None or klines.empty:
            results.append("Veri Alınamadı")
            continue

        outcome = "Açık/Sonuçlanmadı"
        fill_index = -1

        # 1. Giriş kontrolü (Entry Trigger)
        for idx, candle in klines.iterrows():
            if candle['low'] <= entry <= candle['high']:
                fill_index = idx
                break

        if fill_index == -1:
            outcome = "Entry Hiç Gelmedi"
        else:
            # 2. TP veya SL kontrolü (Girişten sonraki mumlar)
            for idx in range(fill_index, len(klines)):
                candle = klines.iloc[idx]

                if side == 'SHORT':
                    # Short: TP aşağıda, SL yukarıda
                    if candle['low'] <= tp:
                        outcome = "✅ TP (KAR)"
                        break
                    elif candle['high'] >= sl:
                        outcome = "❌ SL (ZARAR)"
                        break
                else:  # LONG
                    # Long: TP yukarıda, SL aşağıda
                    if candle['high'] >= tp:
                        outcome = "✅ TP (KAR)"
                        break
                    elif candle['low'] <= sl:
                        outcome = "❌ SL (ZARAR)"
                        break

        print(f"ID: {row['order_id']} | {symbol} | {side} | Sonuç: {outcome}")
        results.append(outcome)

    df['simulation_outcome'] = results
    df.to_csv('data/analyzed_orders.csv', index=False)
    print("\n--- Analiz Tamamlandı. 'data/analyzed_orders.csv' kaydedildi. ---")


# Çalıştır
if __name__ == "__main__":
    analyze_orders('data/closed_orders.csv')