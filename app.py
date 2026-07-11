from flask import Flask, render_template, jsonify, send_from_directory
import requests
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Thread
from datetime import datetime, timezone
import json
import os
import time

app = Flask(__name__)

_session=requests.Session()
_cache={'ts':0,'data':None}
_lock=Lock()
_refresh_lock=Lock()
_position_lock=Lock()
_signal_state_lock=Lock()
_history_lock=Lock()
_positions={}
_history_cache={}
_last_good_pairs = []
_last_good_errors = []
_last_good_fear_greed = None
_background_watcher_started = False
SIGNAL_STATE_FILE = 'signals_state.json'
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TELEGRAM_ALERTS_ENABLED = os.environ.get('TELEGRAM_ALERTS_ENABLED', 'true').lower() == 'true'
BACKGROUND_REFRESH_SECONDS = int(os.environ.get('BACKGROUND_REFRESH_SECONDS', '300'))

INTERVAL = '1d'
SMA_PERIOD = 50

# SMA50 hesaplamak için 50 gün, ek olarak kesişim anını geriye dönük bulabilmek
# için yeterince fazladan gün çekiyoruz. Pencere içinde kesişim bulunamazsa
# (örn. coin SMA'nın hep aynı tarafında kalmışsa) bunu ayrıca işaretliyoruz.
LOOKBACK_FOR_CROSSOVER = 150
TOTAL_CANDLES = SMA_PERIOD + LOOKBACK_FOR_CROSSOVER

# "Geçmiş" / "Performans %" sütunları: sadece o an açık olan son sinyalin değil,
# 2026 başından bu yana üretilmiş TÜM sinyallerin (SMA50 kesişimlerinin) birleşik
# (bileşik/compounded) performansını gösterir. Bu, "son sinyal" metriklerinden
# (K/Z, Pip, Grafik) tamamen ayrı, kendi cache'i olan bir hesaplamadır.
HISTORY_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
HISTORY_START_MS = int(HISTORY_START.timestamp() * 1000)
INTERVAL_MS = {'1d': 24 * 60 * 60 * 1000, '4h': 4 * 60 * 60 * 1000}
INTERVAL_SECONDS = {'1d': 24 * 60 * 60, '4h': 4 * 60 * 60}
KUCOIN_KLINE_TYPE = {'1d': '1day', '4h': '4hour'}
HISTORY_CACHE_SECONDS = int(os.environ.get('HISTORY_CACHE_SECONDS', '3600'))

BASE_PAIRS = [
    {'symbol': 'BTC-USDT', 'label': 'BTC/USDT', 'pip_size': 1},
    {'symbol': 'PAXG-USDT', 'label': 'XAU/USD (PAXG)', 'pip_size': 0.01},
    {'symbol': 'ETH-USDT', 'label': 'ETH/USDT', 'pip_size': 0.1},
    {'symbol': 'SOL-USDT', 'label': 'SOL/USDT', 'pip_size': 0.01},
    {'symbol': 'XRP-USDT', 'label': 'XRP/USDT', 'pip_size': 0.0001},
]
MAJOR_4H_PAIRS = [
    {**pair, 'label': f"{pair['label']} 4H", 'interval': '4h'}
    for pair in BASE_PAIRS
]

TOP_VOLUME_COUNT = 5
STABLE_BASE_ASSETS = {
    'USDC', 'FDUSD', 'TUSD', 'USDP', 'DAI', 'BUSD', 'EUR', 'TRY', 'BRL',
    'GBP', 'AUD', 'BIDR', 'AEUR', 'EURI', 'USTC', 'USD1', 'XUSD', 'PYUSD',
}
LEVERAGED_SUFFIXES = ('UP', 'DOWN', 'BULL', 'BEAR')
EXCLUDED_BASE_ASSETS = {'EIGEN', 'RLUSD', 'ZEC'}


@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory("", "sitemap.xml")


@app.route("/robots.txt")
def robots():
    return send_from_directory("", "robots.txt")


def find_signal_origin(klines):
    """
    klines: KuCoin kline listesi (Binance benzeri formata çevrilmiş), eskiden yeniye
    sıralı, en az SMA_PERIOD+1 eleman.
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
        'origin_is_current_candle': origin_idx == len(days_with_direction) - 1,
        'pl_history': pl_history,
    }


def _normalize_kucoin_klines(raw):
    """KuCoin candles: [time_sec, open, close, high, low, volume, turnover], en yeni önde.
    Geri kalan kodun beklediği Binance benzeri [time_ms, open, high, low, close, volume]
    formatına, eskiden yeniye sıralı şekilde çevirir."""
    converted = [
        [int(row[0]) * 1000, row[1], row[3], row[4], row[2], row[5]]
        for row in raw
    ]
    converted.sort(key=lambda k: k[0])
    return converted


def fetch_klines_since(symbol, interval, start_ms):
    """KuCoin'den start_ms'ten şimdiye kadar TÜM kline'ları sayfalayarak çeker.
    Tek istekteki 1500 mum limitini aşan (örn. 4h aralıkta aylarca veri) durumlar için."""
    interval_seconds = INTERVAL_SECONDS.get(interval, INTERVAL_SECONDS['1d'])
    kucoin_type = KUCOIN_KLINE_TYPE.get(interval, '1day')
    all_klines = []
    cursor = start_ms // 1000
    now_s = int(time.time())

    while cursor < now_s:
        r = _session.get(
            'https://api.kucoin.com/api/v1/market/candles',
            params={'symbol': symbol, 'type': kucoin_type, 'startAt': cursor, 'endAt': now_s},
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get('code') != '200000':
            raise ValueError(payload.get('msg', 'KuCoin veri hatasi'))
        batch = payload.get('data') or []
        if not batch:
            break

        batch = _normalize_kucoin_klines(batch)
        all_klines.extend(batch)
        last_open_time = batch[-1][0]
        if last_open_time // 1000 <= cursor:
            break  # ilerleme yoksa sonsuz döngüye girmemek için dur

        cursor = last_open_time // 1000 + interval_seconds
        if len(batch) < 1500:
            break

    dedup = {k[0]: k for k in all_klines}
    return sorted(dedup.values(), key=lambda k: k[0])


def compute_daily_directions(klines, sma_period):
    """Her gün için kendinden önceki sma_period günün ortalamasına göre yön (True=BUY)."""
    closes = [float(k[4]) for k in klines]
    open_times = [k[0] for k in klines]

    days = []
    for i in range(sma_period, len(closes)):
        window = closes[i - sma_period:i]
        sma_i = sum(window) / sma_period
        price_i = closes[i]
        days.append((open_times[i], price_i, price_i > sma_i, sma_i))
    return days


def compute_signal_history(days, history_start_ms):
    """days: (open_time, close, direction_bool, sma) listesi, eskiden yeniye sıralı.

    history_start_ms'ten bu yana üretilmiş TÜM sinyalleri (yön değişimlerini) bulup
    her birinin kendi giriş/çıkış performansını sırayla bileşik (compounded) olarak
    zincirler. Böylece "şu an açık olan son sinyal" değil, o tarihten bu yana
    açılmış olan TÜM sinyallerin toplam performansı elde edilir.

    Dönen değer: (curve, total_pct) — curve, her gün için kümülatif % değerlerinin
    kronolojik listesi (sparkline için); total_pct, curve'ün son (güncel) değeri.
    Pencerede hiç veri yoksa (None, None) döner.
    """
    window = [d for d in days if d[0] >= history_start_ms]
    if not window:
        return None, None

    segments = []
    seg_start = 0
    current_dir = window[0][2]
    for i in range(1, len(window)):
        if window[i][2] != current_dir:
            segments.append((seg_start, i - 1, current_dir))
            seg_start = i
            current_dir = window[i][2]
    segments.append((seg_start, len(window) - 1, current_dir))

    equity = 1.0
    curve = []
    for seg_start_idx, seg_end_idx, direction in segments:
        entry_price = window[seg_start_idx][1]
        direction_sign = 1 if direction else -1
        day_equity = equity

        for idx in range(seg_start_idx, seg_end_idx + 1):
            day_close = window[idx][1]
            trade_frac = (day_close - entry_price) / entry_price * direction_sign
            day_equity = equity * (1 + trade_frac)
            curve.append((day_equity - 1) * 100)

        equity = day_equity

    total_pct = curve[-1] if curve else 0.0
    return curve, total_pct


def get_signal_history(symbol, interval, sma_period=SMA_PERIOD):
    """2026 başından bu yana bileşik sinyal performansını döndürür (curve, total_pct).

    Bu hesaplama, tam geçmişi sayfalayarak çekmek zorunda olduğundan pahalıdır;
    bu yüzden ana 60 saniyelik yenileme döngüsünden bağımsız, kendi (varsayılan
    1 saatlik) cache'i vardır."""
    cache_key = f'{symbol}:{interval}'

    with _history_lock:
        cached = _history_cache.get(cache_key)
        if cached and time.time() - cached['ts'] < HISTORY_CACHE_SECONDS:
            return cached['curve'], cached['total_pct']

    interval_ms = INTERVAL_MS.get(interval, INTERVAL_MS['1d'])
    buffer_ms = interval_ms * (sma_period + 5)
    fetch_start_ms = HISTORY_START_MS - buffer_ms

    klines = fetch_klines_since(symbol, interval, fetch_start_ms)
    days = compute_daily_directions(klines, sma_period)
    curve, total_pct = compute_signal_history(days, HISTORY_START_MS)

    with _history_lock:
        _history_cache[cache_key] = {'ts': time.time(), 'curve': curve, 'total_pct': total_pct}

    return curve, total_pct


def _is_tradeable_usdt_crypto(symbol_info):
    symbol = symbol_info.get('symbol', '')
    base_asset = symbol_info.get('baseCurrency', '')

    if not symbol_info.get('enableTrading'):
        return False
    if symbol_info.get('quoteCurrency') != 'USDT':
        return False
    if base_asset in STABLE_BASE_ASSETS:
        return False
    if base_asset in EXCLUDED_BASE_ASSETS:
        return False
    if base_asset.endswith(LEVERAGED_SUFFIXES):
        return False
    return symbol.endswith('-USDT')


def _tick_size(symbol_info):
    try:
        value = float(symbol_info.get('priceIncrement') or 0)
        return value or 0.01
    except (TypeError, ValueError):
        return 0.01


def _label_for(symbol_info):
    return f"{symbol_info.get('baseCurrency')}/{symbol_info.get('quoteCurrency')}"


_exchange_info_cache = {'ts': 0, 'data': None}
EXCHANGE_INFO_CACHE_SECONDS = int(os.environ.get('EXCHANGE_INFO_CACHE_SECONDS', '1800'))


def _get_exchange_info():
    now = time.time()
    if _exchange_info_cache['data'] and now - _exchange_info_cache['ts'] < EXCHANGE_INFO_CACHE_SECONDS:
        return _exchange_info_cache['data']

    r = _session.get('https://api.kucoin.com/api/v1/symbols', timeout=10)
    r.raise_for_status()
    payload = r.json()
    if payload.get('code') != '200000':
        raise ValueError(payload.get('msg', 'KuCoin sembol listesi hatasi'))
    data = payload.get('data') or []
    _exchange_info_cache['data'] = data
    _exchange_info_cache['ts'] = now
    return data


def get_pairs():
    pairs = list(BASE_PAIRS)
    known_keys = {(p['symbol'], p.get('interval', INTERVAL)) for p in pairs}

    symbols_data = _get_exchange_info()
    symbols_by_name = {
        item['symbol']: item
        for item in symbols_data
        if _is_tradeable_usdt_crypto(item)
    }

    tickers = _session.get('https://api.kucoin.com/api/v1/market/allTickers', timeout=10)
    tickers.raise_for_status()
    tickers_payload = tickers.json()
    if tickers_payload.get('code') != '200000':
        raise ValueError(tickers_payload.get('msg', 'KuCoin ticker hatasi'))
    ticker_list = (tickers_payload.get('data') or {}).get('ticker') or []

    ranked = sorted(
        (
            ticker for ticker in ticker_list
            if ticker.get('symbol') in symbols_by_name
        ),
        key=lambda ticker: float(ticker.get('volValue') or 0),
        reverse=True,
    )

    for ticker in ranked:
        if len([p for p in pairs if p.get('interval', INTERVAL) == INTERVAL]) >= len(BASE_PAIRS) + TOP_VOLUME_COUNT:
            break

        symbol = ticker['symbol']
        pair_key = (symbol, INTERVAL)
        if pair_key in known_keys:
            continue

        symbol_info = symbols_by_name[symbol]
        pairs.append({
            'symbol': symbol,
            'label': _label_for(symbol_info),
            'pip_size': _tick_size(symbol_info),
        })
        known_keys.add(pair_key)

    pairs.extend(MAJOR_4H_PAIRS)
    return pairs


def stabilize_position(position_key, origin):
    with _position_lock:
        current = _positions.get(position_key)
        same_position = (
            current
            and current['signal'] == origin['signal']
            and current['since'] == origin['since']
        )

        if not same_position:
            current = {
                'signal': origin['signal'],
                'since': origin['since'],
                'entry': origin['entry'],
            }
            _positions[position_key] = current

        return current


def load_signal_state():
    if not os.path.exists(SIGNAL_STATE_FILE):
        return {}

    try:
        with open(SIGNAL_STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def save_signal_state(state):
    with open(SIGNAL_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)


def signal_state_key(pair):
    return f"{pair['symbol']}:{pair.get('interval', INTERVAL)}"


def format_telegram_signal(pair):
    direction_icon = '[BUY]' if pair['signal'] == 'BUY' else '[SELL]'
    label = pair.get('label', pair.get('symbol', ''))
    for suffix in (' 5M', ' 4H', ' 1D'):
        if label.endswith(suffix):
            label = label[:-len(suffix)]
            break
    entry = f"{pair['entry']:,.8f}".rstrip('0').rstrip('.')

    return (
        f"{direction_icon} New signal\n"
        f"{label}\n"
        f"Signal: {pair['signal']}\n"
        f"Entry: {entry}\n"
        "Not financial advice."
    )


def send_telegram_message(text):
    if not (TELEGRAM_ALERTS_ENABLED and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return

    r = _session.post(
        f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage',
        json={
            'chat_id': TELEGRAM_CHAT_ID,
            'text': text,
            'disable_web_page_preview': True,
        },
        timeout=10,
    )
    r.raise_for_status()


def notify_signal_changes(pairs):
    with _signal_state_lock:
        state = load_signal_state()
        next_state = dict(state)
        messages = []

        for pair in pairs:
            key = signal_state_key(pair)
            current = {
                'signal': pair['signal'],
                'since': pair['since'],
            }
            previous = state.get(key)

            if previous and previous != current:
                messages.append(format_telegram_signal(pair))

            next_state[key] = current

        save_signal_state(next_state)

    for message in messages:
        send_telegram_message(message)


def fetch_pair_data(symbol, label, pip_size, interval=INTERVAL):
    interval_seconds = INTERVAL_SECONDS.get(interval, INTERVAL_SECONDS['1d'])
    kucoin_type = KUCOIN_KLINE_TYPE.get(interval, '1day')
    now_s = int(time.time())
    start_at = now_s - interval_seconds * (TOTAL_CANDLES + 2)

    r = _session.get(
        'https://api.kucoin.com/api/v1/market/candles',
        params={'symbol': symbol, 'type': kucoin_type, 'startAt': start_at, 'endAt': now_s},
        timeout=10,
    )
    r.raise_for_status()
    payload = r.json()
    if payload.get('code') != '200000':
        raise ValueError(payload.get('msg', 'KuCoin veri hatasi'))
    raw = _normalize_kucoin_klines(payload.get('data') or [])

    origin = find_signal_origin(raw)
    if origin is None:
        raise ValueError('Yetersiz geçmiş veri')

    sig = origin['signal']
    position_key = f'{symbol}:{interval}'
    position = stabilize_position(position_key, origin)
    entry = position['entry']
    price = origin['price']
    sma = origin['sma']

    direction = 1 if sig == 'BUY' else -1
    pl = (price - entry) / entry * 100 * direction
    pips = (price - entry) / pip_size * direction

    history = [
        {'time': k[0], 'close': float(k[4])}
        for k in raw[-(SMA_PERIOD + 1):]
    ]

    try:
        history_curve, history_pl = get_signal_history(symbol, interval)
    except Exception:
        # "Geçmiş" / "Performans %" opsiyonel bir ek katman; hesaplanamazsa
        # son sinyalin kendi verilerini (K/Z, Pip, Grafik) etkilemesin.
        history_curve, history_pl = None, None

    return {
        'symbol': symbol,
        'label': label,
        'interval': interval,
        'price': price,
        'sma': sma,
        'signal': sig,
        'entry': entry,
        'since': position['since'] / 1000,  # ms -> saniye (frontend saniye bekliyor)
        'since_is_estimate': not origin['complete'],
        'pl': pl,
        'pips': pips,
        'pl_history': origin['pl_history'],
        'history_curve': history_curve or [],
        'history_pl': history_pl,
        'history': history,
    }


def fetch_fear_greed():
    """Alternative.me Crypto Fear & Greed Index'ten son değeri çeker."""
    r = _session.get(
        'https://api.alternative.me/fng/',
        params={'limit': 1, 'format': 'json'},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json().get('data') or []
    if not data:
        raise ValueError('Fear & Greed verisi bos')

    item = data[0]
    return {
        'value': int(item['value']),
        'classification': item.get('value_classification', ''),
        'timestamp': int(item['timestamp']),
    }


def _fetch_all():
    results = []
    errors = []
    try:
        pairs = get_pairs()
    except requests.RequestException as e:
        errors.append({'symbol': 'PAIRS', 'label': 'Pair Listesi', 'error': f'Pair listesi alinamadi: {e}'})
        pairs = list(BASE_PAIRS) + list(MAJOR_4H_PAIRS)
    except Exception as e:
        errors.append({'symbol': 'PAIRS', 'label': 'Pair Listesi', 'error': f'Beklenmeyen hata: {e}'})
        pairs = list(BASE_PAIRS) + list(MAJOR_4H_PAIRS)

    with ThreadPoolExecutor(max_workers=min(10,len(pairs))) as ex:
        futs=[
            ex.submit(
                fetch_pair_data,
                p['symbol'],
                p['label'],
                p['pip_size'],
                p.get('interval', INTERVAL),
            )
            for p in pairs
        ]
        for f,p in zip(futs,pairs):
            try:
                results.append(f.result())
            except requests.RequestException as e:
                errors.append({'symbol': p['symbol'], 'label': p['label'], 'error': f'Veriye ulasilamadi: {e}'})
            except ValueError as e:
                if str(e) != 'Yetersiz geçmiş veri':
                    errors.append({'symbol': p['symbol'], 'label': p['label'], 'error': f'Veri hatasi: {e}'})
            except Exception as e:
                errors.append({'symbol': p['symbol'], 'label': p['label'], 'error': f'Beklenmeyen hata: {e}'})

    try:
        notify_signal_changes(results)
    except requests.RequestException as e:
        errors.append({'symbol': 'TELEGRAM', 'label': 'Telegram', 'error': f'Mesaj gonderilemedi: {e}'})
    except Exception as e:
        errors.append({'symbol': 'TELEGRAM', 'label': 'Telegram', 'error': f'Bildirim hatasi: {e}'})

    fear_greed = None
    try:
        fear_greed = fetch_fear_greed()
    except requests.RequestException as e:
        errors.append({'symbol': 'FNG', 'label': 'Fear & Greed', 'error': f'Veriye ulasilamadi: {e}'})
    except Exception as e:
        errors.append({'symbol': 'FNG', 'label': 'Fear & Greed', 'error': f'Beklenmeyen hata: {e}'})

    if errors and not results:
        cached = get_cached_data()
        if cached['pairs']:
            if fear_greed is None:
                fear_greed = cached['fear_greed']
            cached['fear_greed'] = fear_greed
            return cached

    if fear_greed is None:
        fear_greed = get_cached_data()['fear_greed']

    return {'pairs': results, 'errors': errors, 'fear_greed': fear_greed}


def get_cached_data():
    with _lock:
        return {
            'pairs': list(_last_good_pairs),
            'errors': list(_last_good_errors),
            'fear_greed': _last_good_fear_greed,
        }


@app.route('/')
def index():
    return render_template('index.html')


def fetch_all():
    global _last_good_pairs, _last_good_errors, _last_good_fear_greed

    with _lock:
        if _cache['data'] and time.time()-_cache['ts']<60:
            return _cache['data']

    with _refresh_lock:
        with _lock:
            if _cache['data'] and time.time()-_cache['ts']<60:
                return _cache['data']

        data=_fetch_all()
        with _lock:
            _cache['data']=data;_cache['ts']=time.time()
            _last_good_pairs = list(data.get('pairs', []))
            _last_good_errors = list(data.get('errors', []))
            if data.get('fear_greed') is not None:
                _last_good_fear_greed = data.get('fear_greed')
        return data


def background_signal_watcher():
    while True:
        try:
            fetch_all()
        except Exception:
            pass
        time.sleep(BACKGROUND_REFRESH_SECONDS)


def start_background_signal_watcher():
    global _background_watcher_started

    if _background_watcher_started:
        return

    if not TELEGRAM_ALERTS_ENABLED:
        return

    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug or os.environ.get('RENDER') == 'true':
        Thread(target=background_signal_watcher, daemon=True).start()
        _background_watcher_started = True


start_background_signal_watcher()

@app.route('/api')
def api():
    try:
        return jsonify(fetch_all())
    except Exception as e:
        return jsonify({'pairs': [], 'errors': [{'symbol': 'SERVER', 'label': 'Sunucu', 'error': str(e)}], 'fear_greed': None}), 500


if __name__ == '__main__':
    start_background_signal_watcher()
    app.run(debug=True)