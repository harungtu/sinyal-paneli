"""
app.py — Flask uygulamasi. SADECE orkestrasyon (route'lar, cache, arka plan
dongusu) icerir. Hicbir sinyal algoritmasina ozgu mantik BURADA YASAMAZ.

Sinyal algoritmalari icin bkz. signals/ klasoru (her strateji kendi
dosyasinda; ekleme/cikarma icin signals/__init__.py'daki MODULES listesi
kullanilir). Ortak altyapi (KuCoin baglantisi, pozisyon/sinyal durumu,
Telegram) icin bkz. core.py.
"""

from flask import Flask, render_template, jsonify, send_from_directory
import requests
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Thread
import os
import time

import core
from signals import collect_pairs

app = Flask(__name__)

_cache = {'ts': 0, 'data': None}
_lock = Lock()
_refresh_lock = Lock()
_last_good_pairs = []
_last_good_errors = []
_background_watcher_started = False

BACKGROUND_REFRESH_SECONDS = int(os.environ.get('BACKGROUND_REFRESH_SECONDS', '300'))


@app.route("/sitemap.xml")
def sitemap():
    return send_from_directory("", "sitemap.xml")


@app.route("/robots.txt")
def robots():
    return send_from_directory("", "robots.txt")


def _fetch_all():
    results = []
    errors = []

    try:
        module_pairs = collect_pairs()
    except requests.RequestException as e:
        errors.append({'symbol': 'PAIRS', 'label': 'Pair Listesi', 'error': f'Pair listesi alinamadi: {e}'})
        module_pairs = []
    except Exception as e:
        errors.append({'symbol': 'PAIRS', 'label': 'Pair Listesi', 'error': f'Beklenmeyen hata: {e}'})
        module_pairs = []

    with ThreadPoolExecutor(max_workers=min(10, max(len(module_pairs), 1))) as ex:
        futs = [
            ex.submit(module.fetch, p['symbol'], p['label'], p['pip_size'], p.get('interval', '1d'))
            for module, p in module_pairs
        ]
        for f, (module, p) in zip(futs, module_pairs):
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
        core.notify_signal_changes(results)
    except requests.RequestException as e:
        errors.append({'symbol': 'TELEGRAM', 'label': 'Telegram', 'error': f'Mesaj gonderilemedi: {e}'})
    except Exception as e:
        errors.append({'symbol': 'TELEGRAM', 'label': 'Telegram', 'error': f'Bildirim hatasi: {e}'})

    if errors and not results:
        cached = get_cached_data()
        if cached['pairs']:
            return cached

    return {'pairs': results, 'errors': errors}


def get_cached_data():
    with _lock:
        return {
            'pairs': list(_last_good_pairs),
            'errors': list(_last_good_errors),
        }


@app.route('/')
def index():
    return render_template('index.html')


def fetch_all():
    global _last_good_pairs, _last_good_errors

    with _lock:
        if _cache['data'] and time.time() - _cache['ts'] < 300:
            return _cache['data']

    with _refresh_lock:
        with _lock:
            if _cache['data'] and time.time() - _cache['ts'] < 300:
                return _cache['data']

        data = _fetch_all()
        with _lock:
            _cache['data'] = data
            _cache['ts'] = time.time()
            _last_good_pairs = list(data.get('pairs', []))
            _last_good_errors = list(data.get('errors', []))
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

    if not core.TELEGRAM_ALERTS_ENABLED:
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
        return jsonify({'pairs': [], 'errors': [{'symbol': 'SERVER', 'label': 'Sunucu', 'error': str(e)}]}), 500


if __name__ == '__main__':
    start_background_signal_watcher()
    app.run(debug=True)
