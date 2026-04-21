# Trading Engine

## Kurulum
1. Python 3.10+ kur
2. Sanal ortam oluştur:
   python3 -m venv venv
3. Aktif et:
   source venv/bin/activate
4. Paketleri kur:
   pip install -r requirements.txt
5. .env.example dosyasını .env olarak kopyala
6. Gerekli API anahtarlarını gir

## Çalıştırma
bash ./run.sh

## Durdurma
bash ./stop.sh

## Yeniden başlatma
bash ./restart.sh

## Log izleme
tail -f logs/order.log
tail -f logs/position.log

## News/Sentiment Backtest
Post-news fiyat davranış analizi için:

```bash
python3 news_backtest.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2025-01-01T00:00:00Z \
  --end 2025-03-01T00:00:00Z \
  --events-csv data/news_backtest/events_raw.csv \
  --output-dir data/news_backtest
```

Üretilen `data/news_backtest/news_stats.json` ve `events_enriched.json` dosyaları order engine içinde
filtre + TP yönetim katmanında kullanılır. Veri yoksa trade akışı bloklanmaz.
