from flask import Flask, render_template, jsonify
import requests
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time
_session=requests.Session()
_cache={'ts':0,'data':None}
_lock=Lock()

@app.route("/sitemap.xml")
def sitemap():

    return send_from_directory("", "sitemap.xml")

@app.route("/robots.txt")
def robots():

    return send_from_directory("", "robots.txt")

app = Flask(__name__)

INTERVAL = '1d'
SMA_PERIOD = 50

# SMA50 hesaplamak için 50 gün, ek olarak kesişim anını geriye dönük bulabilmek
# için yeterince fazladan gün çekiyoruz. Pencere içinde kesişim bulunamazsa
# (örn. coin SMA'nın hep aynı tarafında kalmışsa) bunu ayrıca işaretliyoruz.
LOOKBACK_FOR_CROSSOVER = 150
TOTAL_CANDLES = SMA_PERIOD + LOOKBACK_FOR_CROSSOVER

PAIRS = [
    {'symbol': 'BTCUSDT', 'label': 'BTC/USDT', 'pip_size': 1},
    {'symbol': 'PAXGUSDT', 'label': 'XAU/USD (PAXG)', 'pip_size': 0.01},
    {'symbol': 'ETHUSDT', 'label': 'ETH/USDT', 'pip_size': 0.1},
    {'symbol': 'SOLUSDT', 'label': 'SOL/USDT', 'pip_size': 0.01},
    {'symbol': 'XRPUSDT', 'label': 'XRP/USDT', 'pip_size': 0.0001},
]


def find_signal_origin(klines):
    """
    klines: Binance kline listesi, eskiden yeniye sıralı, en az SMA_PERIOD+1 eleman.
    Her gün için o günün kapanışı ile o güne kadarki SMA_PERIOD'luk SMA'sını
    karşılaştırıp BUY/SELL yönünü çıkarır, ardından sondan başa giderek yönün
    en son ne zaman değiştiğini (yani şu anki sinyalin gerçekte başladığı günü) bulur.

    Dönen değer: (signal, entry_price, since_open_time_ms, sma_now, price_now, complete)
    complete=False ise, pencere içinde kesişim bulunamadı; entry/since elimizdeki
    en eski güne ait olup gerçek başlangıç daha öncesi olabilir.
    """
    closes = [float(k[4]) for k in klines]
    open_times = [k[0] for k in klines]

    # Her gün için kendinden önceki SMA_PERIOD günün ortalamasına göre yön (True=BUY)
    days_with_direction = []  # (open_time, close, direction, sma)
    for i in range(SMA_PERIOD, len(closes)):
        window = closes[i - SMA_PERIOD:i]
        sma_i = sum(window) / SMA_PERIOD
        price_i = closes[i]
        direction_i = price_i > sma_i
        days_with_direction.append((open_times[i], price_i, direction_i, sma_i))

    if not days_with_direction:
        return None  # yeterli veri yok

    current_signal_buy = days_with_direction[-1][2]
    sma_now = days_with_direction[-1][3]
    price_now = days_with_direction[-1][1]

    # Sondan başa giderek yönün değiştiği ilk noktayı bul
    origin_idx = 0
    complete = False
    for idx in range(len(days_with_direction) - 1, 0, -1):
        if days_with_direction[idx][2] != days_with_direction[idx - 1][2]:
            origin_idx = idx
            complete = True
            break
    else:
        # Döngü hiç break etmediyse (yön hiç değişmedi), elimizdeki en eski günü kullan
        origin_idx = 0
        complete = False

    origin_open_time, origin_price, _, _ = days_with_direction[origin_idx]

    # Sinyal başlangıcından bugüne kadar her gün için kümülatif K/Z (sparkline için)
    direction_sign = 1 if current_signal_buy else -1
    pl_history = [
        (day_close - origin_price) / origin_price * 100 * direction_sign
        for (_, day_close, _, _) in days_with_direction[origin_idx:]
    ]

    return {
        'signal': 'BUY' if current_signal_buy else 'SELL',
        'entry': origin_price,
        'since': origin_open_time,
        'sma': sma_now,
        'price': price_now,
        'complete': complete,
        'pl_history': pl_history,
    }


def fetch_pair_data(symbol, label, pip_size):
    r = _session.get(
        'https://api.binance.com/api/v3/klines',
        params={'symbol': symbol, 'interval': INTERVAL, 'limit': TOTAL_CANDLES},
        timeout=10,
    )
    r.raise_for_status()
    raw = r.json()

    origin = find_signal_origin(raw)
    if origin is None:
        raise ValueError('Yetersiz geçmiş veri')

    sig = origin['signal']
    entry = origin['entry']
    price = origin['price']
    sma = origin['sma']

    direction = 1 if sig == 'BUY' else -1
    pl = (price - entry) / entry * 100 * direction
    pips = (price - entry) / pip_size * direction

    history = [
        {'time': k[0], 'close': float(k[4])}
        for k in raw[-(SMA_PERIOD + 1):]
    ]

    return {
        'symbol': symbol,
        'label': label,
        'price': price,
        'sma': sma,
        'signal': sig,
        'entry': entry,
        'since': origin['since'] / 1000,  # ms -> saniye (frontend saniye bekliyor)
        'since_is_estimate': not origin['complete'],
        'pl': pl,
        'pips': pips,
        'pl_history': origin['pl_history'],
        'history': history,
    }


def _fetch_all():
    results = []
    errors = []
    with ThreadPoolExecutor(max_workers=min(5,len(PAIRS))) as ex:
        futs=[ex.submit(fetch_pair_data,p['symbol'],p['label'],p['pip_size']) for p in PAIRS]
        for f,p in zip(futs,PAIRS):
            try:
                results.append(f.result())
        except requests.RequestException as e:
            errors.append({'symbol': p['symbol'], 'label': p['label'], 'error': f'Veriye ulasilamadi: {e}'})
        except Exception as e:
            errors.append({'symbol': p['symbol'], 'label': p['label'], 'error': f'Beklenmeyen hata: {e}'})
    return {'pairs': results, 'errors': errors}


@app.route('/')
def index():
    return render_template('index.html')


def fetch_all():
    with _lock:
        if _cache['data'] and time.time()-_cache['ts']<60:
            return _cache['data']
    data=_fetch_all()
    with _lock:
        _cache['data']=data;_cache['ts']=time.time()
    return data

@app.route('/api')
def api():
    return jsonify(fetch_all())


if __name__ == '__main__':
    app.run(debug=True)
