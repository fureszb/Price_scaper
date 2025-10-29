# arkereso/pipelines.py — EGYESÍTETT EXCEL + "NINCS TALÁLAT" + AI FALLBACK
# =============================================================================
# ✅ ÚJDONSÁGOK / VÁLTOZÁSOK:
#   - Egyetlen Excel fájl készül: output_ARKERESO_{timestamp}.xlsx
#   - Minden input × áruház kombináció legalább egy sor (ha nincs találat: "NINCS_TALALAT")
#   - AI validáció ON marad; ha Ollama nem elérhető → gyors FALLBACK értékek:
#       AI_Validacio="HIBA", AI_Pontszam=0, AI_Indoklas="AI nem elérhető (fallback)", AI_Statusz="AI_hiba"
#   - Stabilabb és gyorsabb Ollama hívás (alapértelmezett retries=1, timeout=20s)
#   - Részletesebb logok, átlátható kommentek (# ✅ ÚJDONSÁG, # 🔄 MÓDOSÍTOTT)
# =============================================================================

import json
import time
import requests
import pandas as pd
from scrapy.exceptions import DropItem
import logging
from scrapy import signals
import os
import datetime

# 🔄 MÓDOSÍTOTT: Ollama beállítások – gyorsítás és stabilitás
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3.1"

# ✅ ÚJDONSÁG: Gyorsabb default értékek (csökkentett timeout/retries a teljes futás gyorsításához)
DEFAULT_RETRIES = 1
DEFAULT_TIMEOUT = 20  # másodperc


def _ask_ollama(prompt: str, model: str = MODEL_NAME, retries: int = DEFAULT_RETRIES, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Egyszerű hívó Ollamához (stream off). Visszaadja a teljes szöveges választ.
    Ha nem elérhető, kivételt dob, amit a pipeline fallback-ként kezel."""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.1, "top_p": 0.9}
    }
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.post(OLLAMA_URL, json=payload, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            return data.get("response", "").strip()
        except Exception as e:
            last_err = e
            logging.warning(f"Ollama hívás sikertelen ({attempt + 1}/{retries + 1}): {e}")
            time.sleep(1.2)

    # Sikertelen hívás esetén szándékosan kivétel – ezt a hívó fallback-kel kezeli
    raise Exception(f"Ollama nem elérhető: {last_err}")


def _build_prompt(input_term: str, scraped_name: str, price: str, store: str) -> str:
    return f"""
Feladat: Döntsd el, hogy a TALÁLAT megfelel-e az INPUT termékre.
Válaszolj egyetlen JSON objektummal, pontos kulcsokkal:

{{
  "relevans": "IGEN" vagy "NEM",
  "pontszam": 0..100 (egész),
  "indoklas": "rövid magyar indoklás, max 1-2 mondat"
}}

Döntési szempontok:
- A névben egyező kulcskifejezések (méret, típus, menet/E27/E14, anyag, darabszám).
- Ha nagyon általános a találat, akkor NEM.
- Ha erős egyezés (azonos szabvány, jelölés), akkor IGEN.

INPUT_TERM: "{input_term}"
TALALAT_NEV: "{scraped_name}"
AR: "{price} Ft"
ARUHAZ: "{store}"
""".strip()


class AIValidationPipeline:
    """AI validáció Ollamával (ON) + fallback, ha a modell nem elérhető."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_item(self, item, spider):
        if not getattr(spider, 'enable_ai_validation', True):
            # AI kikapcsolt módban is egységes mezők
            item["AI_Validacio"] = "KIKAPCSOLVA"
            item["AI_Pontszam"] = 0
            item["AI_Indoklas"] = "AI validáció nincs engedélyezve"
            item["AI_Statusz"] = "AI_kikapcsolva"
            return item

        termek = item.get("termek", "")
        nev = item.get("nev", "")
        ar = item.get("ar", "")
        aruhaz = item.get("aruhaz", "")

        # Adatminőség ellenőrzés
        if not all([termek, nev]):
            item["AI_Validacio"] = "HIBA"
            item["AI_Pontszam"] = 0
            item["AI_Indoklas"] = "Hiányzó adat (termek vagy nev üres)"
            item["AI_Statusz"] = "AI_hiba"
            return item

        try:
            prompt = _build_prompt(termek, nev, ar, aruhaz)
            resp = _ask_ollama(prompt)

            relevans = "NEM"
            pontszam = 0
            indok = "N/A"

            try:
                # JSON rész kimetszése laza LLM válaszból
                start_idx = resp.find('{')
                end_idx = resp.rfind('}') + 1
                if start_idx != -1 and end_idx != 0:
                    json_str = resp[start_idx:end_idx]
                    data = json.loads(json_str)
                    relevans = str(data.get("relevans", "NEM")).strip().upper()
                    pontszam = int(data.get("pontszam", 0))
                    indok = str(data.get("indoklas", "")).strip()
                else:
                    # Ha nincs jól formázott JSON, próbáljuk meg heur. módon
                    if "IGEN" in resp.upper():
                        relevans = "IGEN"
                        pontszam = 80
                        indok = "Automatikus elfogadás JSON hiányában"
                    else:
                        relevans = "NEM"
                        pontszam = 30
                        indok = "Automatikus elutasítás JSON hiányában"
            except Exception:
                self.logger.warning(f"⚠️ JSON parse hiba. AI válasz: {resp}")
                if "IGEN" in resp.upper():
                    relevans = "IGEN"
                    pontszam = 80
                    indok = "Automatikus elfogadás JSON hiba miatt"
                else:
                    relevans = "NEM"
                    pontszam = 30
                    indok = "Automatikus elutasítás JSON hiba miatt"

            item["AI_Validacio"] = relevans
            item["AI_Pontszam"] = pontszam
            item["AI_Indoklas"] = indok
            item["AI_Statusz"] = "Elfogadva" if (relevans == "IGEN" and pontszam >= 60) else "Elutasítva_AI_alapjan"

            self.logger.info(f"🤖 AI: {nev} → {relevans} ({pontszam}) - {indok}")
            return item

        except Exception as e:
            # ✅ ÚJDONSÁG: Fallback értékek, hogy az elem NE vesszen el
            self.logger.error(f"⚠️ AI validációs hiba: {e}")
            item["AI_Validacio"] = "HIBA"
            item["AI_Pontszam"] = 0
            item["AI_Indoklas"] = "AI nem elérhető (fallback)"
            item["AI_Statusz"] = "AI_hiba"
            return item


class ExcelPipeline:
    """Egyetlen Excel pipeline – minden áruház és minden termék EGY táblában.
       Garantáljuk: minden INPUT × ÁRUHÁZ kombináció legalább egy sor (NINCS_TALALAT, ha semmi sem jött)."""

    # Áruháznév normalizáló (spider.name → szépen írt áruháznév)
    STORE_NAME_MAP = {
        "bauhaus": "Bauhaus",
        "obi": "OBI",
        "praktiker": "Praktiker",
    }

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        # ✅ ÚJDONSÁG: Globális gyűjtő az ÖSSZES sorhoz (nem áruházanként külön)
        self.rows = []

        # ✅ ÚJDONSÁG: Várható és ténylegesen talált kombinációk (store, term)
        self.expected_combos = set()
        self.found_combos = set()

        # ✅ ÚJDONSÁG: Nyitott spider példányok számolása – a végén egyszer írunk Excel-t
        self.open_spider_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signal=signals.spider_closed)
        return pipeline

    def _store_name(self, spider) -> str:
        """Spider azonosítóból szép áruháznév."""
        return self.STORE_NAME_MAP.get(getattr(spider, "name", "").lower(), getattr(spider, "name", "Ismeretlen"))

    def spider_opened(self, spider):
        self.open_spider_count += 1
        store = self._store_name(spider)
        term = getattr(spider, "termek", "").strip()
        if term:
            # ✅ ÚJDONSÁG: minden spider példány 1 input termékhez tartozik → várjuk ezt a kombinációt
            self.expected_combos.add((store, term))
        self.logger.info(f"📖 Spider elindult: {spider.name} | Áruház: {store} | Termék: '{term}'")

    def process_item(self, item, spider):
        # Normalizált mezők
        aruhaz = item.get("aruhaz") or self._store_name(spider)
        termek = (item.get("termek") or getattr(spider, "termek", "")).strip()
        nev = item.get("nev", "").strip()
        ar = str(item.get("ar", "")).strip()
        url = item.get("url", "")

        # ✅ ÚJDONSÁG: Jegyezzük, hogy erre a (store, term) kombinációra volt találat
        if aruhaz and termek:
            self.found_combos.add((aruhaz, termek))

        excel_item = {
            "aruhaz": aruhaz,
            "termek": termek,
            "nev": nev,
            "ar": ar,
            "url": url,
            "AI_Validacio": item.get("AI_Validacio", "N/A"),
            "AI_Pontszam": item.get("AI_Pontszam", 0),
            "AI_Indoklas": item.get("AI_Indoklas", "N/A"),
            "AI_Statusz": item.get("AI_Statusz", "N/A"),
        }

        # Csak akkor érdemes hozzáadni, ha van név VAGY szeretnénk nyers sorokat is tárolni
        if excel_item["nev"] or excel_item["url"] or excel_item["ar"]:
            self.rows.append(excel_item)
            self.logger.info(f"✅ Sor hozzáadva: [{aruhaz}] {termek} → {nev[:60]}")

        return item

    def _append_no_result_row(self, store: str, term: str):
        """✅ ÚJDONSÁG: 'NINCS TALÁLAT' sor lerakása az adott (store, term) kombinációra."""
        row = {
            "aruhaz": store,
            "termek": term,
            "nev": "NINCS_TALALAT",
            "ar": "-",
            "url": "-",
            "AI_Validacio": "-",
            "AI_Pontszam": 0,
            "AI_Indoklas": "Nincs találat",
            "AI_Statusz": "Nincs találat",
        }
        self.rows.append(row)
        self.logger.info(f"ℹ️ 'NINCS TALÁLAT' sor beszúrva: [{store}] {term}")

    def spider_closed(self, spider):
        """Spider bezárásakor ellenőrizzük az adott (store, term) kombinációt.
           Ha nem volt találat, beszúrjuk a 'NINCS TALÁLAT' sort.
           Az UTOLSÓ spider lezárásakor írjuk ki az EGYETLEN Excel fájlt."""
        store = self._store_name(spider)
        term = getattr(spider, "termek", "").strip()

        self.logger.info(f"🏁 Spider bezárult: {spider.name} | Áruház: {store} | Termék: '{term}'")

        # ✅ ÚJDONSÁG: Ha ezt a kombinációt vártuk, de nem volt találat → 'NINCS TALÁLAT'
        if term and (store, term) in self.expected_combos and (store, term) not in self.found_combos:
            self._append_no_result_row(store, term)

        # Ha ez volt az utolsó spider → írjuk ki az Excel-t
        self.open_spider_count -= 1
        if self.open_spider_count <= 0:
            self._write_single_excel()

    # ✅ ÚJDONSÁG: Egyetlen Excel írása a futás végén
    def _write_single_excel(self):
        if not self.rows:
            self.logger.warning("⚠️ Nincs adat az Excel fájlhoz. Üres fájl nem készül.")
            return

        try:
            output_file = f"output_ARKERESO_{self.timestamp}.xlsx"

            # DataFrame létrehozás
            df = pd.DataFrame(self.rows, columns=[
                "aruhaz",
                "termek",
                "nev",
                "ar",
                "url",
                "AI_Validacio",
                "AI_Pontszam",
                "AI_Indoklas",
                "AI_Statusz",
            ])

            # 🔎 Rendezés: áruház → termek → AI státusz (Elfogadva előre)
            status_order = {"Elfogadva": 0, "Elutasítva_AI_alapjan": 1, "AI_hiba": 2, "KIKAPCSOLVA": 3, "Nincs találat": 4, "N/A": 5}
            df["__status_sort"] = df["AI_Statusz"].map(status_order).fillna(99).astype(int)
            df.sort_values(by=["aruhaz", "termek", "__status_sort", "nev"], inplace=True, kind="stable")
            df.drop(columns=["__status_sort"], inplace=True)

            # Írás Excel-be
            df.to_excel(output_file, index=False)

            # Statisztika (log)
            self.logger.info(f"🎉 {len(df)} sor mentve a közös Excel fájlba: {output_file}")

            # Rövid összesítő
            try:
                summary = df.groupby(["aruhaz", "termek"]).size().to_dict()
                self.logger.info("📊 Sorok száma áruház × termek bontásban:")
                for (store, term), cnt in summary.items():
                    self.logger.info(f"   - {store} | {term}: {cnt} sor")
            except Exception as e_stats:
                self.logger.warning(f"Statisztika készítés hiba: {e_stats}")

        except Exception as e:
            self.logger.error(f"❌ Excel írási hiba: {e}", exc_info=True)
