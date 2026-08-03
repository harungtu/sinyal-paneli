"""
signals/ — Her sinyal algoritmasi kendi dosyasinda, birbirinden bagimsiz
yasar. app.py bu paketi import eder ve TEK TEK stratejileri hic bilmez;
sadece collect_pairs() ve MODULES uzerinden calisir.

YENI BIR STRATEJI EKLEMEK ICIN (app.py'ye VEYA baska hicbir dosyaya
DOKUNMADAN):
  1. signals/senin_stratejin.py dosyasini olustur.
  2. Icinde ASAGIDAKI STANDART ARAYUZU uygula:

       def get_pairs() -> list[dict]:
           '''Bu stratejinin sinyal uretecegi pair'lerin listesi. Her dict:
           {'symbol': 'BTC-USDT', 'label': 'BTC/USDT', 'pip_size': 1,
            'interval': '4h'}  # 'interval' verilmezse '1d' varsayilir.'''
           ...

       def fetch(symbol, label, pip_size, interval) -> dict:
           '''Tek bir pair icin GUNCEL sinyali hesaplar. Donen dict EN AZINDAN
           su alanlari icermeli (frontend'in bekledigi sekil):
             symbol, label, interval, price, sma, signal ('BUY'/'SELL'),
             entry, since (unix saniye), since_is_estimate, pl, pips,
             pl_history (list[float]), history_curve (list[float]),
             history_pl (float|None), history (list[{'time','close'}])
           Ayrica 'strategy': '<benzersiz_isim>' eklemen ONERILIR — boylece
           Telegram bildirim gecmisi baska bir stratejiyle AYNI sembol+
           araligi kullansa bile birbirine karismaz.'''
           ...

  3. Asagidaki MODULES listesine EKLE (import + tek satir).

Bu ikisi disinda (fetch/get_pairs) modulun icinde ISTEDIGIN KADAR yardimci
fonksiyon/sabit tanimlayabilirsin — digerlerini etkilemez. KuCoin'den veri
cekme, pozisyon kararliligi, Telegram bildirimi gibi ORTAK ihtiyaclar icin
core.py'deki hazir fonksiyonlari kullan (core.fetch_recent_klines,
core.fetch_klines_since, core.stabilize_position, core.get_cached_history /
set_cached_history, core.get_top_volume_usdt_pairs, vs.) — tekrar yazma.
"""

from . import sma50_crossover

# --------------------------------------------------------------------------
# KAYITLI STRATEJI MODULLERI — yeni strateji eklerken SADECE bu listeye
# bir satir ekle (yukaridaki import ile birlikte).
# --------------------------------------------------------------------------
MODULES = [
    sma50_crossover,
]


def collect_pairs():
    """Tum kayitli modullerin pair listelerini toplar. Her giris
    (module, pair) ciftidir; boylece _fetch_all, her pair'i HANGI modulun
    fetch() fonksiyonuyla cagiracagini bilir."""
    combined = []
    for module in MODULES:
        for pair in module.get_pairs():
            combined.append((module, pair))
    return combined
