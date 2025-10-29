import scrapy
from scrapy_playwright.page import PageMethod

class PraktikerSpider(scrapy.Spider):
    name = "praktiker"

    def __init__(self, termek="", enable_ai_validation=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.termek = termek
        self.enable_ai_validation = enable_ai_validation

    def start_requests(self):
        url = f"https://www.praktiker.hu/search/{self.termek}"
        yield scrapy.Request(
            url,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    # ✅ Cookie popup elfogadása
                    PageMethod("wait_for_selector", "body", timeout=10000),
                    PageMethod(
                        "evaluate",
                        """
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
                        """
                    ),
                    # ✅ Stabil szelektor a valódi termékkártyákra
                    PageMethod("wait_for_selector", ".prefixbox-product-container .prefixbox-product", timeout=30000)
                ],
            },
            callback=self.parse,
            errback=self.handle_error
        )

    def parse(self, response):
        # ✅ Csak a valós termékkártyákat vesszük (nem a placeholder-eket)
        products = response.css(".prefixbox-product-container .prefixbox-product")[:5]

        if not products:
            self.logger.warning(f"❌ Nincs találat a Praktiker oldalon: {self.termek}")

        for product in products:
            # ✅ Terméknév
            nev = product.css(".prefixbox-product-name::text").get()
            if not nev:
                nev = product.css(".prefixbox-product-name span::text").get()

            # ✅ Ár lekérése biztonságosan
            ar = product.css("#price-with-currency .line-clamp-1::text").get()
            if not ar:
                ar = product.xpath(".//div[@id='price-with-currency']//text()").get()

            # ✅ Link lekérése
            url = product.css("a.pfbx-product-link::attr(href)").get()

            if nev and ar:
                # 🔧 Ár tisztítása
                ar = (
                    ar.replace("\xa0", "")
                    .replace("Ft", "")
                    .replace("/", "")
                    .replace("darab", "")
                    .replace(" ", "")
                    .strip()
                )

                # 🔗 URL kiegészítése
                if url and not url.startswith("http"):
                    url = "https://www.praktiker.hu" + url

                yield {
                    "aruhaz": "Praktiker",
                    "termek": self.termek,
                    "nev": nev.strip(),
                    "ar": ar,
                    "url": url or response.url,
                }
            else:
                self.logger.warning(f"⚠️ Hiányzó adat termék esetén: nev={nev}, ar={ar}, url={url}")

    def handle_error(self, failure):
        self.logger.error(f"⚠️ Hiba a Praktiker lekérés közben: {failure.value}")
