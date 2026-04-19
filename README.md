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