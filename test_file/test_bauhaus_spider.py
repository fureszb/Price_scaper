
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import os
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from arkereso.spiders.praktiker_spider import PraktikerSpider

# 📌 Mentési hely beállítása (automatikusan létrehozza a mappát ha nem létezik)
SAVE_DIR = r"C:\Users\frszb\Desktop\eredmények"
EXCEL_FILE = os.path.join(SAVE_DIR, "Praktiker_eredmenyek.xlsx")

# Globális lista az adatok gyűjtéséhez
talalatok = []

def save_to_excel(data):
    # Mappa létrehozása, ha nem létezik
    os.makedirs(SAVE_DIR, exist_ok=True)

    if not data:
        print("⚠️ Nincsenek menthető adatok.")
        return

    new_data_df = pd.DataFrame(data)

    # Ha a fájl létezik, hozzáfűzzük az új adatokat
    if os.path.exists(EXCEL_FILE):
        existing_df = pd.read_excel(EXCEL_FILE)
        updated_df = pd.concat([existing_df, new_data_df], ignore_index=True)
    else:
        updated_df = new_data_df

    # Mentés Excel-be
    updated_df.to_excel(EXCEL_FILE, index=False)
    print(f"✅ Adatok mentve itt: {EXCEL_FILE}")

def run_bauhaus_spider(termek):
    settings = get_project_settings()

    # 🔍 Debug aktív
    settings.set("PLAYWRIGHTBROWSERTYPE", "firefox")
    settings.set("PLAYWRIGHTLAUNCHOPTIONS", {"headless": False, "slow_mo": 300})
    settings.set("PLAYWRIGHTDEBUG", True)

    # Spider kiterjesztése adatgyűjtéssel
    class BauhausSaveSpider(PraktikerSpider):
        def parse(self, response):
            for item in super().parse(response):
                if isinstance(item, dict):
                    print(f"📥 Mentendő adat: {item}")  # Debug kiírás
                    talalatok.append(item)
                yield item

    process = CrawlerProcess(settings)
    process.crawl(BauhausSaveSpider, termek=termek)
    process.start()

    # Spider futás után mentés
    save_to_excel(talalatok)

if __name__ == "__main__":
    run_bauhaus_spider("Festőszalag")
