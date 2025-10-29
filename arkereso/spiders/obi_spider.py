import scrapy
from scrapy_playwright.page import PageMethod

class ObiSpider(scrapy.Spider):
    name = "obi"

    def __init__(self, termek="", enable_ai_validation=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.termek = termek
        self.enable_ai_validation = enable_ai_validation

    def start_requests(self):
        url = f"https://www.obi.hu/search/{self.termek}"

        # ✅ ÚJ COOKIE ELFogadás és STABIL szelektor várakozás
        yield scrapy.Request(
            url,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    # 1️⃣ Cookie popup bezárása (többféle selector támogatása)
                    PageMethod("wait_for_selector", "body", timeout=10000),
                    PageMethod("evaluate", """
                        () => {
                            const selectors = [
                                '#onetrust-accept-btn-handler',
                                'button:has-text("Elfogadom")',
                                'button.cookie-accept', 
                                '[data-accept*="cookie"]'
                            ];
                            for (const sel of selectors) {
                                const btn = document.querySelector(sel);
                                if (btn) { btn.click(); }
                            }
                        }
                    """),
                    # 2️⃣ Stabil szelektor várakozás
                    PageMethod("wait_for_selector", "ul.products-wp li.product a.product-wrapper", timeout=30000),
                ],
            },
            callback=self.parse,
            errback=self.handle_error,
        )

    def parse(self, response):
        # ✅ Legördülő termékek kiválasztása
        products = response.css("ul.products-wp li.product")[:5]

        if not products:
            self.logger.warning(f"❌ Nincs találat az OBI oldalon: {self.termek}")

        for product in products:
            nev = product.css("span.description p::text").get()

            # ✅ Ár lekérése stabil módszerrel: először attribútum, aztán text fallback
            ar = product.css("span.price-new::attr(data-csscontent)").get()
            if not ar:
                ar = product.css("span.price-new::text").get()

            url = product.css("a.product-wrapper::attr(href)").get()

            if nev and ar:
                ar = ar.replace("\xa0", "").replace("Ft", "").replace("*", "").replace(" ", "").strip()
                if url and not url.startswith("http"):
                    url = "https://www.obi.hu" + url

                yield {
                    "aruhaz": "OBI",
                    "termek": self.termek,
                    "nev": nev.strip(),
                    "ar": ar,
                    "url": url or response.url,
                }

    def handle_error(self, failure):
        self.logger.error(f"⚠️ Hiba az OBI lekérés közben: {failure.value}")
