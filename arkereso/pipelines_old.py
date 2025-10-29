# arkereso/pipelines.py - JAVÍTOTT VERZIÓ
import json
import time
import requests
import pandas as pd
from scrapy.exceptions import DropItem
import logging
from scrapy import signals
import os

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "llama3.1"


class GlobalData:
    """Globális adatgyűjtő osztály"""
    items = []
    excel_written = False


def _ask_ollama(prompt: str, model: str = MODEL_NAME, retries: int = 2, timeout: int = 60) -> str:
    """Egyszerű hívó Ollamához (stream off). Visszaadja a teljes szöveges választ."""
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
            time.sleep(1.5)

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
    """Ollama 3.1 validáció."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_item(self, item, spider):
        if not getattr(spider, 'enable_ai_validation', True):
            item["AI_Validacio"] = "KIKAPCSOLVA"
            item["AI_Pontszam"] = 0
            item["AI_Indoklas"] = "AI validáció nincs engedélyezve"
            item["AI_Statusz"] = "AI_kikapcsolva"
            return item

        termek = item.get("termek", "")
        nev = item.get("nev", "")
        ar = item.get("ar", "")
        aruhaz = item.get("aruhaz", "")

        if not all([termek, nev, ar]):
            item["AI_Validacio"] = "HIBA"
            item["AI_Pontszam"] = 0
            item["AI_Indoklas"] = "Hiányzó adat"
            item["AI_Statusz"] = "AI_hiba"
            return item

        try:
            prompt = _build_prompt(termek, nev, ar, aruhaz)
            resp = _ask_ollama(prompt)

            relevans = "NEM"
            pontszam = 0
            indok = "N/A"

            try:
                start_idx = resp.find('{')
                end_idx = resp.rfind('}') + 1
                if start_idx != -1 and end_idx != 0:
                    json_str = resp[start_idx:end_idx]
                    data = json.loads(json_str)
                    relevans = str(data.get("relevans", "NEM")).strip().upper()
                    pontszam = int(data.get("pontszam", 0))
                    indok = str(data.get("indoklas", "")).strip()
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

            if relevans == "IGEN" and pontszam >= 60:
                item["AI_Statusz"] = "Elfogadva"
            else:
                item["AI_Statusz"] = "Elutasítva_AI_alapjan"

            self.logger.info(f"🤖 AI: {nev} → {relevans} ({pontszam}) - {indok}")
            return item

        except Exception as e:
            self.logger.error(f"⚠️ AI validációs hiba: {e}")
            item["AI_Validacio"] = "HIBA"
            item["AI_Pontszam"] = 0
            item["AI_Indoklas"] = f"Validációs hiba: {str(e)}"
            item["AI_Statusz"] = "AI_hiba"
            return item


class ExcelPipeline:
    """Excel pipeline - minden spider bezárásakor írja ki az adatokat"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.items = []

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signal=signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.logger.info(f"📖 Spider elindult: {spider.name}")

    def process_item(self, item, spider):
        excel_item = {
            "aruhaz": item.get("aruhaz", ""),
            "termek": item.get("termek", ""),
            "nev": item.get("nev", ""),
            "ar": item.get("ar", ""),
            "url": item.get("url", ""),
            "AI_Validacio": item.get("AI_Validacio", "N/A"),
            "AI_Pontszam": item.get("AI_Pontszam", 0),
            "AI_Indoklas": item.get("AI_Indoklas", "N/A"),
            "AI_Statusz": item.get("AI_Statusz", "N/A"),
        }

        if excel_item["nev"] or excel_item["termek"]:
            self.items.append(excel_item)
            GlobalData.items.append(excel_item)
            self.logger.info(f"✅ Termék hozzáadva: {excel_item['nev'][:50]}")

        return item

    def spider_closed(self, spider):
        """Spider bezárásakor írjuk ki az Excel fájlt"""
        self.logger.info(f"🏁 Spider bezárult: {spider.name} - {len(self.items)} termék")

        if not GlobalData.items:
            self.logger.warning("⚠️ Nincs adat az Excel fájlhoz")
            return

        try:
            # Output fájl név meghatározása
            from scrapy.utils.project import get_project_settings
            settings = get_project_settings()
            output_file = settings.get('OUTPUT_FILE', 'output_combined.xlsx')

            # Ha a fájl már létezik, olvassuk be és egyesítsük
            if os.path.exists(output_file):
                existing_df = pd.read_excel(output_file)
                new_df = pd.DataFrame(GlobalData.items)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = pd.DataFrame(GlobalData.items)

            # Duplikátumok eltávolítása
            combined_df = combined_df.drop_duplicates(subset=['aruhaz', 'nev', 'ar'], keep='first')

            # Excel fájl írása
            combined_df.to_excel(output_file, index=False)

            self.logger.info(f"🎉 {len(combined_df)} termék mentve: {output_file}")

            # Statisztika
            by_store = combined_df.groupby('aruhaz').size()
            by_product = combined_df.groupby('termek').size()

            self.logger.info("📊 Statisztika:")
            self.logger.info(f"   Áruházak: {dict(by_store)}")
            self.logger.info(f"   Termékek: {dict(by_product)}")

        except Exception as e:
            self.logger.error(f"❌ Excel írási hiba: {e}")
            import traceback
            traceback.print_exc()