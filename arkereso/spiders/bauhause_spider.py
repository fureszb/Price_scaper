import scrapy
from scrapy_playwright.page import PageMethod

class BauhausSpider(scrapy.Spider):
    name = "bauhaus"
    allowed_domains = ["bauhaus.hu"]

    def __init__(self, termek="", enable_ai_validation=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.termek = termek
        self.enable_ai_validation = enable_ai_validation

    def start_requests(self):
        if not self.termek:
            self.logger.warning("❌ Nincs keresési termék megadva a Bauhaus spidernek.")
            return

        url = "https://www.bauhaus.hu/"
        yield scrapy.Request(
            url=url,
            meta=dict(
                playwright=True,
                playwright_page_methods=[
                    PageMethod("wait_for_selector", "input[type='search']", timeout=10000),
                    PageMethod("fill", "input[type='search']", self.termek),
                    PageMethod("press", "input[type='search']", "Enter"),
                    PageMethod("wait_for_selector", ".prefixbox-product-container", timeout=30000),  # Növelt timeout
                ],
            ),
            callback=self.parse,
            errback=self.handle_error
        )

    def parse(self, response):
        products = response.css(".prefixbox-product-container")[:5]
        print(f"📦 Talált termék dobozok száma: {len(products)}")

        for product in products:
            nev = product.css(".prefixbox-product-name span::text").get()
            ar = product.css(".prefixbox-product-price::text").get()
            url = product.css(".prefixbox-product-name::attr(href)").get()

            if nev and ar:
                nev = nev.strip()
                ar = ar.strip().replace("Ft", "").replace("\xa0", "").replace(" ", "")
                if url and not url.startswith("http"):
                    url = "https://www.bauhaus.hu" + url

                item = {
                    "aruhaz": "Bauhaus",
                    "termek": self.termek,
                    "nev": nev,
                    "ar": ar,
                    "url": url,
                }

                print(f"✅ Mentendő adat: {item}")  # Debug
                yield item
            else:
                print("⚠️ Hiányzó adat egy elemnél")

    def handle_error(self, failure):
        self.logger.error(f"⚠️ Hiba történt a Bauhaus keresésnél: {self.termek}")
        self.logger.error(f"Részletek: {failure.value}")