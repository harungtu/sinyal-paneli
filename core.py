"""
core.py — Tum sinyal modullerinin (signals/*.py) paylastigi ORTAK ALTYAPI.

Burada SADECE strateji-BAGIMSIZ (herhangi bir sinyal algoritmasina ozgu
olmayan) fonksiyonlar bulunur: KuCoin'den veri cekme, mum normallestirme,
pozisyon/sinyal durumu kalicilik, Telegram bildirimi, dinamik "en yuksek
hacimli USDT pariteleri" kesfi.

Bir sinyal algoritmasina OZGU mantik (SMA hesabi, giris/cikis kurallari,
kendi pair listesi, kendi fetch() fonksiyonu vb.) BURAYA EKLENMEZ — o,
signals/ klasorunde kendi dosyasinda yasar. Yeni bir strateji eklerken bu
dosyaya DOKUNMAN GEREKMEMELI; sadece signals/yeni_strateji.py yaz ve
signals/__init__.py'a kaydet.
"""

import json
import os
import time
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import Lock


# --------------------------------------------------------------------------
# HTTP oturumu (KuCoin + Telegram istekleri icin, otomatik retry'li)
# --------------------------------------------------------------------------
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json',
})
_retry_policy = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.7,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=['GET', 'POST'],
)
_adapter = HTTPAdapter(max_retries=_retry_policy)
session.mount('https://', _adapter)
session.mount('http://', _adapter)


# --------------------------------------------------------------------------
# Zaman/aralik sabitleri (tum stratejiler icin ortak)
# --------------------------------------------------------------------------
INTERVAL_MS = {'1d': 24 * 60 * 60 * 1000, '4h': 4 * 60 * 60 * 1000}
INTERVAL_SECONDS = {'1d': 24 * 60 * 60, '4h': 4 * 60 * 60}
KUCOIN_KLINE_TYPE = {'1d': '1day', '4h': '4hour'}

# "Gecmis" / "Performans %" hesaplarinin ortak baslangic noktasi. Tum
# stratejilerin history metriklerinin ADIL KARSILASTIRILABILIR olmasi icin
# (Top 5 siralamasi vb.) HEPSI ayni tarihten baslar.
HISTORY_START = datetime(2024, 1, 1)
HISTORY_START_MS = int(HISTORY_START.timestamp() * 1000)
HISTORY_CACHE_SECONDS = int(os.environ.get('HISTORY_CACHE_SECONDS', '3600'))


# --------------------------------------------------------------------------
# KuCoin kline (mum) yardimcilari
# --------------------------------------------------------------------------
def normalize_kucoin_klines(raw):
    """KuCoin candles: [time_sec, open, close, high, low, volume, turnover], en yeni önde.
    Geri kalan kodun beklediği Binance benzeri [time_ms, open, high, low, close, volume]
    formatına, eskiden yeniye sıralı şekilde çevirir."""
    converted = [
        [int(row[0]) * 1000, row[1], row[3], row[4], row[2], row[5]]
        for row in raw
    ]
    converted.sort(key=lambda k: k[0])
    return converted


def drop_unclosed_candle(klines, interval_ms, now_ms=None):
    """KuCoin /market/candles, istek anina kadar (endAt=now) veri cektigimizde
    henuz KAPANMAMIS (o an devam eden) son mumu da listeye dahil ediyor; bu
    mumun 'close' degeri sabit degil, sorgu anindaki canli fiyat.

    Sinyal yonu (BUY/SELL) SADECE kapanmis mumlara gore belirlenmeli; aksi
    halde fiyat mum icinde gidip geldikce sinyal "flip-flop" yapar ve
    gercekte olmayan (fake) Telegram bildirimleri tetiklenir. Bu fonksiyon,
    son mum henuz kapanmamissa onu listeden cikarir.
    """
    if not klines:
        return klines
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    if klines[-1][0] + interval_ms > now_ms:
        return klines[:-1]
    return klines


def fetch_recent_klines(symbol, interval, total_candles, timeout=15):
    """Son 'total_candles' kadar KAPANMIS mumu tek sayfada ceker (KuCoin'in tek
    istekte ~1500 mum limitini asmayan, 'guncel sinyal' hesaplari icin yeterli
    kisa pencereler icin kullanilir). Uzun/tarihsel pencereler icin bunun
    yerine fetch_klines_since() kullan (sayfalama yapar)."""
    interval_seconds = INTERVAL_SECONDS.get(interval, INTERVAL_SECONDS['1d'])
    kucoin_type = KUCOIN_KLINE_TYPE.get(interval, '1day')
    now_s = int(time.time())
    start_at = now_s - interval_seconds * (total_candles + 2)

    r = session.get(
        'https://api.kucoin.com/api/v1/market/candles',
        params={'symbol': symbol, 'type': kucoin_type, 'startAt': start_at, 'endAt': now_s},
        timeout=timeout,
    )
    r.raise_for_status()
    payload = r.json()
    if payload.get('code') != '200000':
        raise ValueError(payload.get('msg', 'KuCoin veri hatasi'))

    raw = normalize_kucoin_klines(payload.get('data') or [])
    interval_ms = INTERVAL_MS.get(interval, INTERVAL_MS['1d'])
    return drop_unclosed_candle(raw, interval_ms, now_ms=now_s * 1000)


def fetch_klines_since(symbol, interval, start_ms):
    """start_ms'ten bugune KADAR TUM kapanmis mumlari sayfalayarak ceker.
    'Gecmis' / 'Performans %' gibi uzun pencereli hesaplar icin kullanilir."""
    interval_seconds = INTERVAL_SECONDS.get(interval, INTERVAL_SECONDS['1d'])
    kucoin_type = KUCOIN_KLINE_TYPE.get(interval, '1day')

    all_klines = []
    end_at = int(time.time())
    page_span_seconds = interval_seconds * 1500  # bir sayfada en fazla ~1500 mum

    while True:
        start_at = max(0, end_at - page_span_seconds)

        r = session.get(
            "https://api.kucoin.com/api/v1/market/candles",
            params={
                "symbol": symbol,
                "type": kucoin_type,
                "startAt": start_at,
                "endAt": end_at,
            },
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
        if payload.get("code") != "200000":
            raise ValueError(payload.get("msg"))

        batch = payload.get("data") or []
        if not batch:
            # Bu pencerede hic veri yok; borsanin elindeki en eski veriye
            # ulasmis olabiliriz. Daha da geriye gitmenin anlami yok.
            break

        batch = normalize_kucoin_klines(batch)
        all_klines.extend(batch)

        oldest = batch[0][0] // 1000
        if oldest * 1000 <= start_ms:
            break

        end_at = oldest - interval_seconds
        if start_at <= 0:
            break

    dedup = {k[0]: k for k in all_klines}
    klines = sorted(dedup.values(), key=lambda x: x[0])
    klines = [k for k in klines if k[0] >= start_ms]

    if not klines:
        return klines

    interval_ms = INTERVAL_MS.get(interval, INTERVAL_MS["1d"])
    return drop_unclosed_candle(klines, interval_ms)


# --------------------------------------------------------------------------
# Gecmis (history) hesaplari icin ortak cache
# --------------------------------------------------------------------------
_history_lock = Lock()
_history_cache = {}


def get_cached_history(cache_key):
    with _history_lock:
        cached = _history_cache.get(cache_key)
        if cached and time.time() - cached['ts'] < HISTORY_CACHE_SECONDS:
            return cached['curve'], cached['total_pct']
    return None


def set_cached_history(cache_key, curve, total_pct):
    with _history_lock:
        _history_cache[cache_key] = {'ts': time.time(), 'curve': curve, 'total_pct': total_pct}


# --------------------------------------------------------------------------
# Pozisyon kararliligi (ayni sinyal surerken entry/since'in ufak veri
# dalgalanmalariyla degismemesi icin kilitlenir)
# --------------------------------------------------------------------------
_position_lock = Lock()
_positions = {}


def stabilize_position(position_key, origin):
    """origin (yeni hesaplanan {signal, since, entry}) bir onceki ile AYNI
    sinyal + baslangic zamanina sahipse, kayitli (kilitli) entry/since
    degerlerini korur; farkliysa yeni pozisyonu kaydeder."""
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


def clear_position(position_key):
    with _position_lock:
        _positions.pop(position_key, None)


# --------------------------------------------------------------------------
# Sinyal durumu kaliciligi (Telegram bildirimlerinin sadece GERCEK
# degisimlerde tetiklenmesi icin diske yazilir)
# --------------------------------------------------------------------------
SIGNAL_STATE_FILE = 'signals_state.json'
_signal_state_lock = Lock()


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
    """Pair'i benzersiz sekilde tanimlar. Ayni sembol farkli stratejiler/
    araliklarla kullanilabildigi icin (orn. BTC-USDT hem 1D hem 4H'de ayri
    ayri), anahtara interval'i (ve varsa strateji adini) dahil ediyoruz."""
    parts = [pair['symbol'], pair.get('interval', '1d')]
    if pair.get('strategy'):
        parts.append(pair['strategy'])
    return ':'.join(parts)


# --------------------------------------------------------------------------
# Telegram bildirimleri
# --------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TELEGRAM_ALERTS_ENABLED = os.environ.get('TELEGRAM_ALERTS_ENABLED', 'true').lower() == 'true'

# Etiketlerin sonundan atilacak, "hangi strateji/aralik" bilgisini tasiyan
# ekler. Yeni bir strateji modulu kendi etiket son ekini (orn. ' Benim Strateji')
# eklerse buraya da ekleyebilir; eklemezse sadece etiket kisaltilmadan kalir
# (islevsellik bozulmaz, sadece Telegram mesaji biraz daha uzun olur).
LABEL_SUFFIXES_TO_STRIP = (' 5M', ' 4H', ' 1D')


def format_telegram_signal(pair):
    direction_icon = '[BUY]' if pair['signal'] == 'BUY' else '[SELL]'
    label = pair.get('label', pair.get('symbol', ''))
    for suffix in LABEL_SUFFIXES_TO_STRIP:
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

    r = session.post(
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
    """Sinyal listesini bir onceki calisma ile karsilastirir, GERCEKTEN
    degisen (yeni acilan) pozisyonlar icin Telegram mesaji yollar.

    Bir pair'in signal='WAIT' olmasi ya da entry'sinin olmamasi (henuz acik
    pozisyon yok) durumunda bildirim gonderilmez, sadece durum guncellenir —
    boylece format_telegram_signal'a eksik veri gitmez."""
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

            if pair.get('signal') == 'WAIT' or pair.get('entry') is None:
                next_state[key] = current
                continue

            if previous and previous != current:
                messages.append(format_telegram_signal(pair))

            next_state[key] = current

        save_signal_state(next_state)

    for message in messages:
        send_telegram_message(message)


# --------------------------------------------------------------------------
# Dinamik "en yuksek hacimli USDT paritesi" kesfi (birden fazla strateji
# "ilk N BASE_PAIRS disindaki en likit coinleri de ekle" isteyebilir diye
# ortak bir yardimci olarak burada tutuluyor)
# --------------------------------------------------------------------------
STABLE_BASE_ASSETS = {
    'USDC', 'FDUSD', 'TUSD', 'USDP', 'DAI', 'BUSD', 'EUR', 'TRY', 'BRL',
    'GBP', 'AUD', 'BIDR', 'AEUR', 'EURI', 'USTC', 'USD1', 'XUSD', 'PYUSD',
}
LEVERAGED_SUFFIXES = ('UP', 'DOWN', 'BULL', 'BEAR')
EXCLUDED_BASE_ASSETS = {'EIGEN', 'RLUSD', 'ZEC'}

_exchange_info_cache = {'ts': 0, 'data': None}
EXCHANGE_INFO_CACHE_SECONDS = int(os.environ.get('EXCHANGE_INFO_CACHE_SECONDS', '1800'))


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


def get_exchange_info():
    now = time.time()
    if _exchange_info_cache['data'] and now - _exchange_info_cache['ts'] < EXCHANGE_INFO_CACHE_SECONDS:
        return _exchange_info_cache['data']

    r = session.get('https://api.kucoin.com/api/v1/symbols', timeout=10)
    r.raise_for_status()
    payload = r.json()
    if payload.get('code') != '200000':
        raise ValueError(payload.get('msg', 'KuCoin sembol listesi hatasi'))
    data = payload.get('data') or []
    _exchange_info_cache['data'] = data
    _exchange_info_cache['ts'] = now
    return data


def get_top_volume_usdt_pairs(exclude_symbols, count):
    """exclude_symbols disinda kalan, KuCoin'de islem gorebilen USDT
    paritelerinden 24s hacme gore ilk 'count' tanesini dondurur.
    Her biri {'symbol','label','pip_size'} seklinde bir dict'tir."""
    symbols_data = get_exchange_info()
    symbols_by_name = {
        item['symbol']: item
        for item in symbols_data
        if _is_tradeable_usdt_crypto(item)
    }

    tickers = session.get('https://api.kucoin.com/api/v1/market/allTickers', timeout=10)
    tickers.raise_for_status()
    tickers_payload = tickers.json()
    if tickers_payload.get('code') != '200000':
        raise ValueError(tickers_payload.get('msg', 'KuCoin ticker hatasi'))
    ticker_list = (tickers_payload.get('data') or {}).get('ticker') or []

    ranked = sorted(
        (t for t in ticker_list if t.get('symbol') in symbols_by_name),
        key=lambda t: float(t.get('volValue') or 0),
        reverse=True,
    )

    result = []
    for ticker in ranked:
        if len(result) >= count:
            break
        symbol = ticker['symbol']
        if symbol in exclude_symbols:
            continue
        symbol_info = symbols_by_name[symbol]
        result.append({
            'symbol': symbol,
            'label': _label_for(symbol_info),
            'pip_size': _tick_size(symbol_info),
        })

    return result
