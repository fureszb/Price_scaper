# main.py - JAVÍTOTT VERZIÓ
import argparse
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import datetime
import os
import sys

# Biztosítsuk, hogy a projekt root elérhető
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)


def main():
    # === Parancssori argumentumok kezelése ===
    parser = argparse.ArgumentParser(description="Árkereső futtató")
    parser.add_argument("--debug", type=int, default=0, help="1 = látható böngésző, 0 = headless")
    args = parser.parse_args()

    # Excel input ellenőrzés
    input_file = "input.xlsx"
    if not os.path.exists(input_file):
        print(f"❌ Nem található input fájl: {input_file}")
        return

    df = pd.read_excel(input_file)
    if "termek" not in df.columns:
        print("❌ Az input.xlsx fájl nem tartalmaz 'termek' oszlopot.")
        return

    termek_lista = df["termek"].dropna().tolist()
    if not termek_lista:
        print("❌ Nincs feldolgozható termék az input.xlsx fájlban.")
        return

    # Projekt settings betöltése vagy alapértelmezett beállítások
    try:
        settings = get_project_settings()
    except:
        print("⚠️ Nem található settings fájl, alapértelmezett beállítások használata")
        settings = {
            'BOT_NAME': 'arkereso',
            'SPIDER_MODULES': ['arkereso.spiders'],
            'NEWSPIDER_MODULE': 'arkereso.spiders',
            'ROBOTSTXT_OBEY': False,
            'PLAYWRIGHT_LAUNCH_OPTIONS': {
                "headless": not bool(args.debug),
            },
            'ITEM_PIPELINES': {
                'arkereso.pipelines.AIValidationPipeline': 300,
                'arkereso.pipelines.ExcelPipeline': 800,
            },
            'DOWNLOADER_MIDDLEWARES': {
                'scrapy_playwright.middleware.PlaywrightMiddleware': 800,
            },
            'CONCURRENT_REQUESTS': 1,
            'DOWNLOAD_DELAY': 1,
        }

    # Playwright beállítások
    debug_mode = bool(args.debug)
    settings.set("PLAYWRIGHT_LAUNCH_OPTIONS", {
        "headless": not debug_mode,
        "slow_mo": 500 if debug_mode else 0
    })

    print(f"🔧 Debug mód: {'LÁTHATÓ' if debug_mode else 'HEADLESS'}")
    print(f"🔍 Keresett termékek: {', '.join(termek_lista)}")

    # Timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"🕒 Futtatás időpontja: {timestamp}")

    # Scrapy process
    process = CrawlerProcess(settings)

    # Spider-ek importálása
    try:
        from arkereso.spiders.bauhause_spider import BauhausSpider
        from arkereso.spiders.obi_spider import ObiSpider
        from arkereso.spiders.praktiker_spider import PraktikerSpider

        # Összes termék lekérdezés futtatása minden spiderrel
        for termek in termek_lista:
            print(f"🚀 Keresés indítása: {termek}")
            process.crawl(BauhausSpider, termek=termek)
            process.crawl(ObiSpider, termek=termek)
            process.crawl(PraktikerSpider, termek=termek)

        # Folyamat indítása
        print("🎬 Scrapy folyamat indítása...")
        process.start()
        print("✅ Scrapy folyamat befejeződött!")

    except Exception as e:
        print(f"❌ Hiba a spider-ek indításakor: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()