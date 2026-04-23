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

## Event sample'ları doldurma yolları

Order katmanında görülen `market_event_sample_size` ve `event_miner_sample_size` metriklerini artırmak için
3 temel yol var:

1. **Market event backtest üretmek (fiyat/volume shock tabanlı):**

```bash
python3 market_event_backtest.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2025-01-01T00:00:00Z \
  --end 2025-03-01T00:00:00Z \
  --output-dir data/market_event_backtest
```

Bu komut `data/market_event_backtest/market_event_stats.json` üretir ve event bucket bazında `sample_size`
hesaplar.

2. **News event dataset büyütmek (CSV + opsiyonel CryptoPanic):**

```bash
python3 news_backtest.py \
  --symbols BTCUSDT,ETHUSDT \
  --start 2025-01-01T00:00:00Z \
  --end 2025-03-01T00:00:00Z \
  --events-csv data/news_backtest/events_raw.csv \
  --with-cryptopanic \
  --output-dir data/news_backtest
```

Periyot uzadıkça ve event sayısı arttıkça `news_stats.json` içindeki `sample_size` alanları artar.

3. **Event miner istatistiklerini geçmiş candle'lardan offline üretmek:**

`event_miner.build_event_stats_from_candles(...)` fonksiyonu geçmiş candle serisinden event-key bazlı
istatistik üretir. Üretilen sözlük `event_miner.load_event_stats(...)` ile belleğe yüklenerek
`event_miner_sample_size` doldurulabilir.

Pratik öneri: Önce 90-180 gün veriyle batch üretim yapıp, ardından haftalık incremental backtest ile
sample'ları güncel tutun.
