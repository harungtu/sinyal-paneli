"""
signals/sma50_crossover.py — "Fiyat 4 saatlik/1 gunluk mumu SMA50'nin
yukarisina keserse AL, asagisina keserse SAT" stratejisi.

STRATEJI KURALI (kullaniciyla birlikte netlestirilen versiyon):
  - Fiyat (mum kapanisi) SMA50'yi YUKARI keserse -> AL (BUY) pozisyonu acilir.
  - Fiyat SMA50'yi ASAGI keserse -> SAT (SELL) pozisyonu acilir.
  - AL pozisyonu, fiyat SMA50'yi asagi kestiginde kapanir; kapanis SIRASINDA
    AYNI ANDA (ayni mumun kapanisinda) SAT pozisyonu acilir. SAT icin de
    simetrik olarak ayni kural gecerlidir (asagi->yukari kesiste SAT kapanir,
    ayni anda AL acilir).
  - Giris/cikis DAIMA mumun KAPANISINDA ve KAPANIS FIYATINDAN yapilir; mum
    henuz kapanmadan (o an devam ederken) sinyal degismez.
  - Bu kural hem 4 saatlik hem 1 gunluk mumlar icin AYNIDIR; tek fark
    kullanilan mum araligidir (interval='4h' / '1d').

Bu dosyadaki fonksiyonlar HEM "guncel sinyal" (find_signal_origin, fetch())
HEM "2020'den bu yana toplam performans" (compute_daily_directions,
compute_signal_history, get_signal_history) hesaplarini icerir — ikisi de
AYNI SMA/kesisim mantigini kullanir, boylece ekranda gosterilen sinyal ile
Telegram'a giden sinyal ve "Performance %" sutunu HER ZAMAN TUTARLI kalir.
"""

import time

import core
DEBUG_EXPORT_SYMBOL = "BTC-USDT"
DEBUG_EXPORT_INTERVAL = "4h"
DEBUG_EXPORT_FILE = "history_debug.txt"

INTERVAL = '1d'
SMA_PERIOD = 50

# SMA50 hesaplamak için 50 gün, ek olarak kesişim anını geriye dönük bulabilmek
# için yeterince fazladan gün çekiyoruz. Pencere içinde kesişim bulunamazsa
# (örn. coin SMA'nın hep aynı tarafında kalmışsa) bunu ayrıca işaretliyoruz.
LOOKBACK_FOR_CROSSOVER = 150
TOTAL_CANDLES = SMA_PERIOD + LOOKBACK_FOR_CROSSOVER

TOP_VOLUME_COUNT = 5

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


# --------------------------------------------------------------------------
# Guncel sinyal hesabi
# --------------------------------------------------------------------------
def find_signal_origin(klines):
    """
    klines: KuCoin kline listesi (Binance benzeri formata çevrilmiş), eskiden yeniye
    sıralı, en az SMA_PERIOD+1 eleman.
    Her mum için o mumun kapanışı ile o muma kadarki SMA_PERIOD'luk SMA'sını
    karşılaştırıp BUY/SELL yönünü çıkarır, ardından sondan başa giderek yönün
    en son ne zaman değiştiğini (yani şu anki sinyalin gerçekte başladığı mumu) bulur.

    Dönen değer: dict(signal, entry, since, sma, price, complete, pl_history)
    complete=False ise, pencere içinde kesişim bulunamadı; entry/since elimizdeki
    en eski muma ait olup gerçek başlangıç daha öncesi olabilir.
    """
    closes = [float(k[4]) for k in klines]
    open_times = [k[0] for k in klines]

    # Her mum için kendinden önceki SMA_PERIOD mumun ortalamasına göre yön (True=BUY)
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
        # Döngü hiç break etmediyse (yön hiç değişmedi), elimizdeki en eski muma bak
        origin_idx = 0
        complete = False

    origin_open_time, origin_price, _, _ = days_with_direction[origin_idx]

    # Sinyal başlangıcından bugüne kadar her mum için kümülatif K/Z (sparkline için)
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


# --------------------------------------------------------------------------
# "2020'den bu yana toplam performans" (Top 5 / Performance % sutunu) hesabi
# --------------------------------------------------------------------------
def compute_daily_directions(klines, sma_period=SMA_PERIOD):
    """Her mum için kendinden önceki sma_period mumun ortalamasına göre yön (True=BUY).
    find_signal_origin ile AYNI SMA/yon mantigini kullanir (tutarlilik icin)."""
    closes = [float(k[4]) for k in klines]
    open_times = [k[0] for k in klines]

    days = []
    for i in range(sma_period, len(closes)):
        window = closes[i - sma_period:i]
        sma_i = sum(window) / sma_period
        price_i = closes[i]
        days.append((open_times[i], price_i, price_i > sma_i, sma_i))
    return days


def compute_signal_history(days, history_start_ms, symbol=None, interval=None):

    window = [d for d in days if d[0] >= history_start_ms]

    if not window:
        return None, None

    debug = (
        symbol == DEBUG_EXPORT_SYMBOL
        and interval == DEBUG_EXPORT_INTERVAL
    )

    curve = []
    cumulative_pct = 0.0

    current_direction = window[0][2]
    direction_sign = 1 if current_direction else -1

    entry_time = window[0][0]
    entry_price = window[0][1]

    out = None

    if debug:
        out = open(DEBUG_EXPORT_FILE, "w", encoding="utf8")
        out.write(
            f"History Debug\n"
            f"Symbol : {symbol}\n"
            f"Interval : {interval}\n\n"
        )

    for i in range(len(window)):

        close_time = window[i][0]
        close_price = window[i][1]

        current_trade_pct = (
            (close_price - entry_price)
            / entry_price
            * 100
            * direction_sign
        )

        curve.append(cumulative_pct + current_trade_pct)

        # yön değişti
        if i > 0 and window[i][2] != window[i - 1][2]:

            if debug:

                # İlk satırda başlık yaz
                if out.tell() == len(
                    f"History Debug\nSymbol : {symbol}\nInterval : {interval}\n\n"
                ):
                    out.write(
                        f"{'POS':<6}"
                        f"{'ENTRY TIME':<22}"
                        f"{'EXIT TIME':<22}"
                        f"{'ENTRY':>12}"
                        f"{'EXIT':>12}"
                        f"{'TRADE %':>12}"
                        f"{'HISTORY %':>12}\n"
                    )
                    out.write("-" * 100 + "\n")

                out.write(
                    f"{('BUY' if current_direction else 'SELL'):<6}"
                    f"{time.strftime('%Y-%m-%d %H:%M', time.gmtime(entry_time/1000)):<22}"
                    f"{time.strftime('%Y-%m-%d %H:%M', time.gmtime(close_time/1000)):<22}"
                    f"{entry_price:>12.4f}"
                    f"{close_price:>12.4f}"
                    f"{current_trade_pct:>11.2f}%"
                    f"{(cumulative_pct + current_trade_pct):>11.2f}%\n")

            cumulative_pct += current_trade_pct

            current_direction = window[i][2]
            direction_sign = 1 if current_direction else -1

            entry_time = close_time
            entry_price = close_price

            curve[-1] = cumulative_pct

    if debug:

        out.write("\n")
        out.write("-" * 100 + "\n")
        out.write(f"{'TOTAL HISTORY PERFORMANCE':<74}{cumulative_pct:>11.2f}%\n")

        out.close()

    total_pct = curve[-1] if curve else 0.0

    return curve, total_pct

def get_signal_history(symbol, interval, sma_period=SMA_PERIOD):
    """2020 başından bu yana bileşik sinyal performansını döndürür (curve, total_pct).

    Bu hesaplama, tam geçmişi sayfalayarak çekmek zorunda olduğundan pahalıdır;
    bu yüzden ana yenileme döngüsünden bağımsız, core.py'deki ortak cache'i kullanır."""
    cache_key = f'{symbol}:{interval}:sma50'

    cached = core.get_cached_history(cache_key)
    if cached is not None:
        return cached

    interval_ms = core.INTERVAL_MS.get(interval, core.INTERVAL_MS['1d'])
    buffer_ms = interval_ms * (sma_period + 5)
    fetch_start_ms = core.HISTORY_START_MS - buffer_ms

    klines = core.fetch_klines_since(symbol, interval, fetch_start_ms)
    days = compute_daily_directions(klines, sma_period)

    curve, total_pct = compute_signal_history(days, core.HISTORY_START_MS, symbol=symbol, interval=interval)

    core.set_cached_history(cache_key, curve, total_pct)
    return curve, total_pct


# --------------------------------------------------------------------------
# Pair listesi (BASE_PAIRS + dinamik en-yuksek-hacimli 5 coin + 4H versiyonlari)
# --------------------------------------------------------------------------
def get_pairs():
    pairs = list(BASE_PAIRS)
    existing_symbols = {p['symbol'] for p in pairs}

    top_volume = core.get_top_volume_usdt_pairs(existing_symbols, TOP_VOLUME_COUNT)
    pairs.extend(top_volume)

    pairs.extend(MAJOR_4H_PAIRS)
    return pairs


# --------------------------------------------------------------------------
# Bu modulun DISARIYA ACTIGI STANDART ARAYUZ: fetch()
# app.py / signals registry, her pair icin bunu cagirir. Donen dict'in
# sekli TUM stratejiler icin AYNI olmalidir (bkz. signals/__init__.py).
# --------------------------------------------------------------------------
def fetch(symbol, label, pip_size, interval=INTERVAL):
    raw = core.fetch_recent_klines(symbol, interval, TOTAL_CANDLES, timeout=10)

    origin = find_signal_origin(raw)
    if origin is None:
        raise ValueError('Yetersiz geçmiş veri')

    sig = origin['signal']
    position_key = core.signal_state_key({'symbol': symbol, 'interval': interval, 'strategy': 'sma50_crossover'})
    position = core.stabilize_position(position_key, origin)
    entry = position['entry']

    # Gosterilen fiyat/P&L icin en guncel (kapanmis) mumun fiyatini kullaniyoruz;
    # yon/entry/since yukarida SADECE kapanmis mumlardan geldi.
    price = float(raw[-1][4]) if raw else origin['price']
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
        # ana sinyalin kendi verilerini (K/Z, Pip, Grafik) etkilemesin.
        history_curve, history_pl = None, None

    return {
        'symbol': symbol,
        'label': label,
        'interval': interval,
        # 'strategy': notify_signal_changes'in Telegram durumunu ILERIDE ayni
        # sembol+araligi kullanan BASKA bir strateji moduluyle KARISTIRMAMASI
        # icin. Frontend bu alani kullanmaz, gormezden gelir.
        'strategy': 'sma50_crossover',
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
